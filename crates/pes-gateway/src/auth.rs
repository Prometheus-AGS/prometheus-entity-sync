//! JWT validation: HMAC-SHA256 (shared secret) or RS256 (JWKS URL, with a
//! 5-minute key cache).

use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use pes_core::{SyncError, TokenClaims};
use tokio::sync::RwLock;

/// How JWTs presented by clients should be validated.
#[derive(Debug, Clone)]
pub enum JwtValidationConfig {
    /// Validate with a shared HMAC-SHA256 secret.
    HmacSha256 {
        /// The shared secret used to verify the JWT's HS256 signature.
        secret: String,
    },
    /// Validate with RS256, fetching public keys from a JWKS endpoint.
    /// Keys are cached for [`JWKS_CACHE_TTL`].
    Rs256Jwks {
        /// The JWKS endpoint URL to fetch RS256 public keys from.
        jwks_url: String,
    },
}

/// How long a fetched JWKS key set is cached before being re-fetched.
pub const JWKS_CACHE_TTL: Duration = Duration::from_secs(5 * 60);

#[derive(Debug, serde::Deserialize)]
struct Jwk {
    kid: Option<String>,
    n: String,
    e: String,
}

#[derive(Debug, serde::Deserialize)]
struct JwkSet {
    keys: Vec<Jwk>,
}

/// Validates client JWTs per a [`JwtValidationConfig`], caching JWKS keys
/// when in RS256 mode.
pub struct JwtValidator {
    config: JwtValidationConfig,
    jwks_cache: DashMap<String, (Arc<DecodingKey>, Instant)>,
    http_client: reqwest::Client,
    // Serializes concurrent JWKS refreshes for the same kid so a burst of
    // connections during a cache-miss doesn't fire N redundant HTTP fetches.
    refresh_lock: RwLock<()>,
}

impl JwtValidator {
    /// Construct a validator for `config`.
    pub fn new(config: JwtValidationConfig) -> Self {
        Self {
            config,
            jwks_cache: DashMap::new(),
            http_client: reqwest::Client::new(),
            refresh_lock: RwLock::new(()),
        }
    }

    /// Validate `token` and return its claims.
    ///
    /// Returns [`SyncError::AuthError`] for any failure: expired,
    /// malformed, wrong signature, or (RS256 mode) an unknown/unfetchable
    /// key id. No internal detail about *why* validation failed is
    /// distinguishable from the error variant alone — see
    /// `pes-gateway::error::GatewayErrorResponse` for the client-facing
    /// redaction boundary applied on top of this.
    pub async fn validate(&self, token: &str) -> Result<TokenClaims, SyncError> {
        match &self.config {
            JwtValidationConfig::HmacSha256 { secret } => {
                self.validate_hmac(token, secret)
            }
            JwtValidationConfig::Rs256Jwks { jwks_url } => {
                self.validate_rs256(token, jwks_url).await
            }
        }
    }

    fn validate_hmac(&self, token: &str, secret: &str) -> Result<TokenClaims, SyncError> {
        let key = DecodingKey::from_secret(secret.as_bytes());
        let validation = Validation::new(Algorithm::HS256);
        decode::<TokenClaims>(token, &key, &validation)
            .map(|data| data.claims)
            .map_err(|e| SyncError::AuthError(format!("HMAC validation failed: {e}")))
    }

    async fn validate_rs256(&self, token: &str, jwks_url: &str) -> Result<TokenClaims, SyncError> {
        let header = decode_header(token)
            .map_err(|e| SyncError::AuthError(format!("malformed JWT header: {e}")))?;
        let kid = header
            .kid
            .ok_or_else(|| SyncError::AuthError("JWT header missing kid".to_string()))?;

        let key = self.get_or_fetch_key(jwks_url, &kid).await?;

        let mut validation = Validation::new(Algorithm::RS256);
        validation.validate_exp = true;
        decode::<TokenClaims>(token, &key, &validation)
            .map(|data| data.claims)
            .map_err(|e| SyncError::AuthError(format!("RS256 validation failed: {e}")))
    }

    async fn get_or_fetch_key(
        &self,
        jwks_url: &str,
        kid: &str,
    ) -> Result<Arc<DecodingKey>, SyncError> {
        if let Some(entry) = self.jwks_cache.get(kid) {
            let (key, fetched_at) = entry.value();
            if fetched_at.elapsed() < JWKS_CACHE_TTL {
                return Ok(Arc::clone(key));
            }
        }

        // Only one concurrent refresh at a time; everyone else waits for it
        // and then re-checks the cache (which will now be warm).
        let _guard = self.refresh_lock.write().await;

        // Re-check: another task may have refreshed while we waited for the lock.
        if let Some(entry) = self.jwks_cache.get(kid) {
            let (key, fetched_at) = entry.value();
            if fetched_at.elapsed() < JWKS_CACHE_TTL {
                return Ok(Arc::clone(key));
            }
        }

        let jwk_set: JwkSet = self
            .http_client
            .get(jwks_url)
            .send()
            .await
            .map_err(|e| SyncError::AuthError(format!("JWKS fetch failed: {e}")))?
            .json()
            .await
            .map_err(|e| SyncError::AuthError(format!("JWKS response parse failed: {e}")))?;

        for jwk in &jwk_set.keys {
            let key_kid = jwk.kid.as_deref().unwrap_or("");
            let decoding_key = DecodingKey::from_rsa_components(&jwk.n, &jwk.e)
                .map_err(|e| SyncError::AuthError(format!("invalid JWK RSA components: {e}")))?;
            let arc_key = Arc::new(decoding_key);
            self.jwks_cache
                .insert(key_kid.to_string(), (Arc::clone(&arc_key), Instant::now()));
        }

        self.jwks_cache
            .get(kid)
            .map(|entry| Arc::clone(&entry.value().0))
            .ok_or_else(|| SyncError::AuthError(format!("no JWK found for kid '{kid}'")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use jsonwebtoken::{encode, EncodingKey, Header};
    use rsa::pkcs1::EncodeRsaPrivateKey;
    use rsa::traits::PublicKeyParts;
    use rsa::RsaPrivateKey;
    use std::collections::HashMap;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn claims(exp_offset_secs: i64) -> TokenClaims {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        TokenClaims {
            sub: "user-1".to_string(),
            tenant_id: None,
            exp: (now + exp_offset_secs).max(0) as u64,
            custom: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn hmac_accepts_a_validly_signed_unexpired_token() {
        let validator = JwtValidator::new(JwtValidationConfig::HmacSha256 {
            secret: "test-secret".to_string(),
        });
        let token = encode(
            &Header::new(Algorithm::HS256),
            &claims(3600),
            &EncodingKey::from_secret(b"test-secret"),
        )
        .unwrap();

        let result = validator.validate(&token).await.unwrap();
        assert_eq!(result.sub, "user-1");
    }

    #[tokio::test]
    async fn hmac_rejects_a_token_signed_with_the_wrong_secret() {
        let validator = JwtValidator::new(JwtValidationConfig::HmacSha256 {
            secret: "correct-secret".to_string(),
        });
        let token = encode(
            &Header::new(Algorithm::HS256),
            &claims(3600),
            &EncodingKey::from_secret(b"wrong-secret"),
        )
        .unwrap();

        let err = validator.validate(&token).await.unwrap_err();
        assert!(matches!(err, SyncError::AuthError(_)));
    }

    #[tokio::test]
    async fn hmac_rejects_an_expired_token() {
        let validator = JwtValidator::new(JwtValidationConfig::HmacSha256 {
            secret: "test-secret".to_string(),
        });
        let token = encode(
            &Header::new(Algorithm::HS256),
            &claims(-3600),
            &EncodingKey::from_secret(b"test-secret"),
        )
        .unwrap();

        let err = validator.validate(&token).await.unwrap_err();
        assert!(matches!(err, SyncError::AuthError(_)));
    }

    #[tokio::test]
    async fn hmac_rejects_a_malformed_token() {
        let validator = JwtValidator::new(JwtValidationConfig::HmacSha256 {
            secret: "test-secret".to_string(),
        });
        let err = validator.validate("not-a-jwt").await.unwrap_err();
        assert!(matches!(err, SyncError::AuthError(_)));
    }

    fn rsa_keypair() -> RsaPrivateKey {
        let mut rng = rsa::rand_core::OsRng;
        RsaPrivateKey::new(&mut rng, 2048).unwrap()
    }

    fn jwk_json(private_key: &RsaPrivateKey, kid: &str) -> serde_json::Value {
        let public_key = private_key.to_public_key();
        let n = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(public_key.n().to_bytes_be());
        let e = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(public_key.e().to_bytes_be());
        serde_json::json!({ "kid": kid, "kty": "RSA", "n": n, "e": e })
    }

    #[tokio::test]
    async fn rs256_fetches_jwks_and_accepts_a_validly_signed_token() {
        let private_key = rsa_keypair();
        let jwks_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/jwks"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "keys": [jwk_json(&private_key, "key-1")]
            })))
            .expect(1)
            .mount(&jwks_server)
            .await;

        let validator = JwtValidator::new(JwtValidationConfig::Rs256Jwks {
            jwks_url: format!("{}/jwks", jwks_server.uri()),
        });

        let der = private_key.to_pkcs1_der().unwrap();
        let encoding_key = EncodingKey::from_rsa_der(der.as_bytes());
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some("key-1".to_string());
        let token = encode(&header, &claims(3600), &encoding_key).unwrap();

        let result = validator.validate(&token).await.unwrap();
        assert_eq!(result.sub, "user-1");
    }

    #[tokio::test]
    async fn rs256_reuses_cached_key_without_refetching_jwks() {
        let private_key = rsa_keypair();
        let jwks_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/jwks"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "keys": [jwk_json(&private_key, "key-1")]
            })))
            .expect(1)
            .mount(&jwks_server)
            .await;

        let validator = JwtValidator::new(JwtValidationConfig::Rs256Jwks {
            jwks_url: format!("{}/jwks", jwks_server.uri()),
        });

        let der = private_key.to_pkcs1_der().unwrap();
        let encoding_key = EncodingKey::from_rsa_der(der.as_bytes());
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some("key-1".to_string());
        let token = encode(&header, &claims(3600), &encoding_key).unwrap();

        validator.validate(&token).await.unwrap();
        // Second validate with the same kid must hit the cache, not the
        // JWKS endpoint again — `.expect(1)` above would fail the test on drop otherwise.
        validator.validate(&token).await.unwrap();
    }

    #[tokio::test]
    async fn rs256_rejects_a_token_with_no_kid_in_header() {
        let jwks_server = MockServer::start().await;
        let validator = JwtValidator::new(JwtValidationConfig::Rs256Jwks {
            jwks_url: format!("{}/jwks", jwks_server.uri()),
        });

        let private_key = rsa_keypair();
        let der = private_key.to_pkcs1_der().unwrap();
        let encoding_key = EncodingKey::from_rsa_der(der.as_bytes());
        let token = encode(&Header::new(Algorithm::RS256), &claims(3600), &encoding_key).unwrap();

        let err = validator.validate(&token).await.unwrap_err();
        assert!(matches!(err, SyncError::AuthError(_)));
    }

    #[tokio::test]
    async fn rs256_rejects_a_token_whose_kid_is_not_in_the_jwks_response() {
        let signing_key = rsa_keypair();
        let other_key = rsa_keypair();
        let jwks_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/jwks"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "keys": [jwk_json(&other_key, "key-other")]
            })))
            .mount(&jwks_server)
            .await;

        let validator = JwtValidator::new(JwtValidationConfig::Rs256Jwks {
            jwks_url: format!("{}/jwks", jwks_server.uri()),
        });

        let der = signing_key.to_pkcs1_der().unwrap();
        let encoding_key = EncodingKey::from_rsa_der(der.as_bytes());
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some("key-missing".to_string());
        let token = encode(&header, &claims(3600), &encoding_key).unwrap();

        let err = validator.validate(&token).await.unwrap_err();
        assert!(matches!(err, SyncError::AuthError(_)));
    }
}
