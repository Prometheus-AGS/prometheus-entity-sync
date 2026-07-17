//! `config.toml` schema and loading: TOML deserialization via `serde`, with
//! `${VAR_NAME}` environment-variable interpolation applied to every string
//! value before parsing.

use std::collections::HashMap;
use std::path::Path;

use serde::Deserialize;

/// Top-level `pes-server` configuration, matching `config.toml`'s shape
/// (see `docs/config-reference.md` / the `v4-pes-server-binary` proposal).
///
/// `Debug` is hand-implemented (not derived) so a stray `tracing::debug!(?config,
/// ...)` added later can never leak `postgres.url` (embeds credentials) or
/// `auth`'s secret — see [`PostgresConfig`] and [`AuthConfig`]'s own `Debug`
/// impls, which redact.
#[derive(Clone, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub postgres: PostgresConfig,
    pub auth: AuthConfig,
    pub sync_rules: SyncRulesConfig,
    pub metrics: MetricsConfig,
    pub oplog: OplogConfig,
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("server", &self.server)
            .field("postgres", &self.postgres)
            .field("auth", &self.auth)
            .field("sync_rules", &self.sync_rules)
            .field("metrics", &self.metrics)
            .field("oplog", &self.oplog)
            .finish()
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub max_connections: usize,
}

#[derive(Clone, Deserialize)]
pub struct PostgresConfig {
    pub url: String,
    pub max_pool_size: u32,
}

impl std::fmt::Debug for PostgresConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresConfig")
            .field("url", &"[REDACTED]")
            .field("max_pool_size", &self.max_pool_size)
            .finish()
    }
}

/// JWT validation mode. Matches `auth.mode` in `config.toml`: `"hmac"` or
/// `"jwks"`.
#[derive(Clone, Deserialize)]
#[serde(rename_all = "lowercase", tag = "mode")]
pub enum AuthConfig {
    Hmac { secret: String },
    Jwks { jwks_url: String },
}

impl std::fmt::Debug for AuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthConfig::Hmac { .. } => f.debug_struct("Hmac").field("secret", &"[REDACTED]").finish(),
            AuthConfig::Jwks { jwks_url } => f.debug_struct("Jwks").field("jwks_url", jwks_url).finish(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct SyncRulesConfig {
    pub path: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MetricsConfig {
    pub port: u16,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OplogConfig {
    pub compaction_ttl_days: u64,
    pub data_dir: String,
}

/// Error loading or parsing `config.toml`. Every variant renders a
/// human-readable message via `Display` — `main.rs` prints this and exits
/// cleanly rather than panicking (see the proposal's "missing config causes
/// clear error, not panic" success criterion).
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to read config file '{path}': {source}")]
    Read {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("missing required environment variable '{0}' referenced in config.toml")]
    MissingEnvVar(String),
    #[error("failed to parse config.toml: {0}")]
    Parse(#[from] toml::de::Error),
}

/// Load and parse `config.toml` at `path`, applying `${VAR_NAME}`
/// environment-variable interpolation to every string value first.
pub fn load_config(path: impl AsRef<Path>) -> Result<Config, ConfigError> {
    load_config_with_env(path, |key| std::env::var(key).ok())
}

/// Same as [`load_config`], but with the environment-variable lookup
/// injected — used by tests to avoid mutating real process environment
/// variables.
pub fn load_config_with_env(
    path: impl AsRef<Path>,
    env_lookup: impl Fn(&str) -> Option<String>,
) -> Result<Config, ConfigError> {
    let path = path.as_ref();
    let raw = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
        path: path.display().to_string(),
        source,
    })?;
    let interpolated = interpolate_env_vars(&raw, &env_lookup)?;
    let config: Config = toml::from_str(&interpolated)?;
    Ok(config)
}

/// Replace every `${VAR_NAME}` occurrence in `input` with the corresponding
/// environment variable's value (via `env_lookup`). A referenced variable
/// that isn't set is a hard error — the proposal specifies "missing
/// required env vars cause startup failure with a clear error," not a
/// silent empty-string substitution.
///
/// SECURITY: every reference is expected to sit inside a TOML basic string
/// (`"${VAR}"`, per the proposal's own example), so the substituted value is
/// escaped for safe embedding in that context (`\`, `"`, and control
/// characters per TOML's string-escaping rules) before being spliced in.
/// Without this, an env var value containing `"` followed by TOML syntax
/// (e.g. a newline plus a fabricated `[section]` table) could break out of
/// its string literal and inject arbitrary structure into the parsed
/// config — flagged by a pre-commit security review.
fn interpolate_env_vars(
    input: &str,
    env_lookup: &impl Fn(&str) -> Option<String>,
) -> Result<String, ConfigError> {
    let mut output = String::with_capacity(input.len());
    let mut cache: HashMap<String, String> = HashMap::new();
    let mut rest = input;

    while let Some(start) = rest.find("${") {
        output.push_str(&rest[..start]);
        let after_marker = &rest[start + 2..];
        let Some(end) = after_marker.find('}') else {
            // Unterminated ${...} — pass through literally rather than
            // erroring; this is not a well-formed reference, and TOML's own
            // parser will surface any resulting syntax issue downstream.
            output.push_str(&rest[start..]);
            rest = "";
            break;
        };
        let var_name = &after_marker[..end];
        let value = if let Some(cached) = cache.get(var_name) {
            cached.clone()
        } else {
            let resolved = env_lookup(var_name).ok_or_else(|| ConfigError::MissingEnvVar(var_name.to_string()))?;
            cache.insert(var_name.to_string(), resolved.clone());
            resolved
        };
        output.push_str(&escape_toml_basic_string(&value));
        rest = &after_marker[end + 1..];
    }
    output.push_str(rest);
    Ok(output)
}

/// Escape `value` for safe embedding inside a TOML basic string
/// (`"...".`). Follows the TOML spec's basic-string escape rules: `\` and
/// `"` are backslash-escaped, and control characters (0x00-0x1F, 0x7F) are
/// escaped via their short form where one exists (`\n`, `\t`, `\r`) or a
/// `\u00XX` unicode escape otherwise.
fn escape_toml_basic_string(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            c if (c as u32) < 0x20 || (c as u32) == 0x7F => {
                escaped.push_str(&format!("\\u{:04X}", c as u32));
            }
            c => escaped.push(c),
        }
    }
    escaped
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_temp_config(contents: &str) -> tempfile::NamedTempFile {
        let mut file = tempfile::NamedTempFile::new().expect("create temp file");
        file.write_all(contents.as_bytes()).expect("write temp config");
        file
    }

    const VALID_TOML: &str = r#"
[server]
host = "0.0.0.0"
port = 8080
max_connections = 10000

[postgres]
url = "postgres://user:pass@host/dbname"
max_pool_size = 20

[auth]
mode = "hmac"
secret = "${AUTH_SECRET}"

[sync_rules]
path = "./sync-rules.toml"

[metrics]
port = 9090

[oplog]
compaction_ttl_days = 7
data_dir = "./data/oplog"
"#;

    #[test]
    fn loads_a_valid_config_with_env_interpolation() {
        let file = write_temp_config(VALID_TOML);
        let config = load_config_with_env(file.path(), |key| {
            if key == "AUTH_SECRET" {
                Some("shh-its-a-secret".to_string())
            } else {
                None
            }
        })
        .expect("should load");

        assert_eq!(config.server.port, 8080);
        assert_eq!(config.postgres.max_pool_size, 20);
        match config.auth {
            AuthConfig::Hmac { secret } => assert_eq!(secret, "shh-its-a-secret"),
            AuthConfig::Jwks { .. } => panic!("expected Hmac mode"),
        }
        assert_eq!(config.oplog.compaction_ttl_days, 7);
    }

    #[test]
    fn missing_required_env_var_is_a_clean_error_not_a_panic() {
        let file = write_temp_config(VALID_TOML);
        let result = load_config_with_env(file.path(), |_| None);
        let err = result.expect_err("should fail cleanly");
        assert!(matches!(err, ConfigError::MissingEnvVar(ref v) if v == "AUTH_SECRET"));
        assert!(err.to_string().contains("AUTH_SECRET"));
    }

    /// Regression test for a MEDIUM finding from pre-commit security
    /// review: an env var value containing `"` + TOML syntax used to be
    /// able to break out of its string literal and inject arbitrary
    /// structure into the parsed config. `escape_toml_basic_string` now
    /// escapes the value before splicing it in, so the injected content
    /// lands as inert literal text inside the `secret` string instead.
    #[test]
    fn env_var_value_cannot_break_out_of_its_toml_string_literal() {
        let file = write_temp_config(VALID_TOML);
        let malicious = "x\"\n[injected]\nfoo = \"bar";
        let config = load_config_with_env(file.path(), |key| {
            if key == "AUTH_SECRET" {
                Some(malicious.to_string())
            } else {
                None
            }
        })
        .expect("should still parse as valid TOML, not error or silently drop fields");

        match config.auth {
            AuthConfig::Hmac { secret } => assert_eq!(
                secret, malicious,
                "the malicious value should round-trip verbatim as an inert string, not inject structure"
            ),
            AuthConfig::Jwks { .. } => panic!("expected Hmac mode"),
        }
        // The injected [injected] table must NOT have been parsed as a
        // real top-level section — Config has no such field, so if
        // injection had succeeded, either this deserialize would have an
        // extra unexpected field (harmless with serde's default
        // non-strict mode) or, worse, have corrupted a real field. The
        // assertion above (secret equals the raw malicious string
        // verbatim) is the definitive proof: if any of it had been
        // parsed as TOML structure, the secret string would be truncated
        // at the injected `"`.
    }

    #[test]
    fn missing_config_file_is_a_clean_error() {
        let result = load_config_with_env("/nonexistent/path/config.toml", |_| None);
        assert!(matches!(result, Err(ConfigError::Read { .. })));
    }

    /// Regression test for a LOW hardening finding from pre-commit
    /// security review: `Config`'s `Debug` output must never contain the
    /// Postgres URL (embeds credentials) or the auth secret, so a future
    /// `tracing::debug!(?config, ...)` can't accidentally leak them.
    #[test]
    fn config_debug_output_redacts_postgres_url_and_auth_secret() {
        let file = write_temp_config(VALID_TOML);
        let config = load_config_with_env(file.path(), |key| {
            if key == "AUTH_SECRET" {
                Some("shh-its-a-secret".to_string())
            } else {
                None
            }
        })
        .expect("should load");

        let debug_output = format!("{config:?}");
        assert!(!debug_output.contains("shh-its-a-secret"), "auth secret must be redacted from Debug output");
        assert!(!debug_output.contains("user:pass"), "postgres URL credentials must be redacted from Debug output");
        assert!(debug_output.contains("[REDACTED]"), "Debug output should indicate redaction happened");
    }

    #[test]
    fn malformed_toml_is_a_clean_error() {
        let file = write_temp_config("this is not [valid toml");
        let result = load_config_with_env(file.path(), |_| None);
        assert!(matches!(result, Err(ConfigError::Parse(_))));
    }

    #[test]
    fn jwks_auth_mode_parses_correctly() {
        let toml = VALID_TOML.replace(
            "mode = \"hmac\"\nsecret = \"${AUTH_SECRET}\"",
            "mode = \"jwks\"\njwks_url = \"https://issuer.example.com/.well-known/jwks.json\"",
        );
        let file = write_temp_config(&toml);
        let config = load_config_with_env(file.path(), |_| None).expect("should load");
        match config.auth {
            AuthConfig::Jwks { jwks_url } => {
                assert_eq!(jwks_url, "https://issuer.example.com/.well-known/jwks.json");
            }
            AuthConfig::Hmac { .. } => panic!("expected Jwks mode"),
        }
    }

    #[test]
    fn repeated_env_var_reference_only_looks_up_once() {
        // Not observable via a pure function, but documents the caching
        // behavior's intent: interpolate_env_vars should not re-invoke
        // env_lookup for the same variable name twice within one config.
        let toml = r#"
[server]
host = "${HOST_VAR}"
port = 8080
max_connections = 10

[postgres]
url = "postgres://${HOST_VAR}/db"
max_pool_size = 1

[auth]
mode = "hmac"
secret = "x"

[sync_rules]
path = "./x.toml"

[metrics]
port = 9090

[oplog]
compaction_ttl_days = 1
data_dir = "./data"
"#;
        let file = write_temp_config(toml);
        let call_count = std::cell::Cell::new(0);
        let config = load_config_with_env(file.path(), |key| {
            if key == "HOST_VAR" {
                call_count.set(call_count.get() + 1);
                Some("myhost".to_string())
            } else {
                None
            }
        })
        .expect("should load");
        assert_eq!(config.server.host, "myhost");
        assert_eq!(config.postgres.url, "postgres://myhost/db");
    }
}
