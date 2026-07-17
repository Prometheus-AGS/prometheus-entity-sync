//! `BucketAssigner` — the security boundary between JWT claims and bucket
//! data access. See `docs/sync-rules-reference.md` and the module-level
//! security notes in [`crate::template`] for the full threat model.
//!
//! # Non-negotiable invariants
//!
//! 1. No string interpolation of user-controlled values into SQL. Every
//!    parameter query is executed with the JWT `sub` bound as `$1` via
//!    `sqlx::query` bind parameters — never `format!`.
//! 2. Template substitution (`{bucket_parameters.X}`) only ever inserts
//!    values that passed [`crate::template::validate_safe_value`].
//! 3. Expired or otherwise invalid JWTs are rejected before any database
//!    call is made.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use pes_core::{BucketAssignment, BucketId, SyncError, TokenClaims};
use sqlx::{PgPool, Row};
use tokio::task::JoinHandle;

use crate::error::ParseError;
use crate::parser::SyncRuleSet;
use crate::{template, validator};

/// Cache key: the JWT subject plus the rule set version this assignment
/// was resolved against, so a `sync-rules.toml` reload invalidates stale
/// cache entries instead of silently serving assignments from an old ruleset.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CacheKey {
    sub: String,
    rule_set_version: String,
}

/// Resolves JWT claims to the [`BucketAssignment`]s a user is authorized to
/// subscribe to, by executing each [`crate::parser::SyncRuleSet`] rule's
/// parameter queries against Postgres.
pub struct BucketAssigner {
    rule_set: Arc<SyncRuleSet>,
    pool: PgPool,
    cache: Arc<DashMap<CacheKey, (Vec<BucketAssignment>, Instant)>>,
    cache_ttl: Duration,
}

impl BucketAssigner {
    /// Construct a new assigner backed by `pool`, resolving buckets from
    /// `rule_set` and caching results for `cache_ttl`.
    ///
    /// `rule_set` is validated via [`crate::validate`] before the assigner
    /// is constructed — it is structurally impossible to build a
    /// `BucketAssigner` from a `SyncRuleSet` that failed semantic
    /// validation (missing parameter_queries entries, wrong placeholder
    /// count, undeclared `bucket_parameters` references, or invalid bucket
    /// ids). This closes the gap between "the validator exists" and "the
    /// validator is actually enforced on the runtime path."
    pub fn new(rule_set: Arc<SyncRuleSet>, pool: PgPool, cache_ttl: Duration) -> Result<Self, ParseError> {
        validator::validate(&rule_set)?;
        Ok(Self {
            rule_set,
            pool,
            cache: Arc::new(DashMap::new()),
            cache_ttl,
        })
    }

    /// Resolve the bucket assignments authorized for `claims`.
    ///
    /// Returns an empty vec (not an error) when the claims are valid but no
    /// rule's parameter queries produce a match. Returns
    /// [`SyncError::AuthError`] for expired claims, and
    /// [`SyncError::Database`] if a parameter query fails.
    pub async fn assign(&self, claims: &TokenClaims) -> Result<Vec<BucketAssignment>, SyncError> {
        self.check_not_expired(claims)?;

        let cache_key = CacheKey {
            sub: claims.sub.clone(),
            rule_set_version: self.rule_set.version.clone(),
        };
        if let Some(cached) = self.cache_lookup(&cache_key) {
            return Ok(cached);
        }

        let mut assignments = Vec::new();
        for rule in self.rule_set.rules.values() {
            if let Some(assignment) = self.resolve_rule(rule, claims).await? {
                assignments.push(assignment);
            }
        }

        self.cache
            .insert(cache_key, (assignments.clone(), Instant::now()));
        Ok(assignments)
    }

    fn check_not_expired(&self, claims: &TokenClaims) -> Result<(), SyncError> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| SyncError::AuthError(format!("system clock error: {e}")))?
            .as_secs();
        if claims.exp <= now {
            return Err(SyncError::AuthError(format!(
                "JWT expired: exp={}, now={now}",
                claims.exp
            )));
        }
        Ok(())
    }

    fn cache_lookup(&self, key: &CacheKey) -> Option<Vec<BucketAssignment>> {
        let entry = self.cache.get(key)?;
        let (assignments, inserted_at) = entry.value();
        if inserted_at.elapsed() < self.cache_ttl {
            Some(assignments.clone())
        } else {
            None
        }
    }

    /// Remove all cache entries older than `cache_ttl`, returning the number
    /// of entries evicted.
    ///
    /// [`Self::cache_lookup`] already ignores stale entries on read, but
    /// never removes them — a client presenting many validly-signed JWTs
    /// with distinct `sub` claims (or repeated `sync-rules.toml` reloads,
    /// which change `rule_set_version`) can otherwise grow the cache
    /// unboundedly. Call this periodically (see [`Self::spawn_cache_sweeper`])
    /// to bound cache memory in production.
    pub fn sweep_expired_entries(&self) -> usize {
        let before = self.cache.len();
        self.cache
            .retain(|_, (_, inserted_at)| inserted_at.elapsed() < self.cache_ttl);
        before - self.cache.len()
    }

    /// Spawn a background task that calls [`Self::sweep_expired_entries`]
    /// every `interval` until the returned [`JoinHandle`] is aborted or
    /// dropped-and-detached. Callers own the handle's lifecycle — e.g.
    /// `pes-gateway` should abort it on server shutdown.
    pub fn spawn_cache_sweeper(self: &Arc<Self>, interval: Duration) -> JoinHandle<()> {
        let assigner = Arc::clone(self);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            // The first tick fires immediately; skip it so we don't sweep an
            // empty cache the instant the task starts.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                let evicted = assigner.sweep_expired_entries();
                if evicted > 0 {
                    tracing::debug!(evicted, "swept expired bucket-assignment cache entries");
                }
            }
        })
    }

    /// Given a changed entity's type and row data, determine which
    /// currently-cached [`BucketAssignment`]s should receive it — without a
    /// Postgres round-trip.
    ///
    /// # Local evaluation scope (important limitation)
    ///
    /// `data_queries` are arbitrary SQL `SELECT` statements (e.g. `SELECT *
    /// FROM entities WHERE owner_id = {bucket_parameters.user_id}`), and
    /// this method does NOT implement a general SQL `WHERE`-clause
    /// evaluator. Instead it checks the common single-owner-column pattern
    /// used throughout `docs/sync-rules-reference.md`: for each cached
    /// assignment whose `data_queries` contains an entry named
    /// `entity_type`, the assignment is considered a match if any of its
    /// resolved `parameters` values equals the value of `owner_column` in
    /// `row_data` (tried against every resolved parameter, since the
    /// column name used in a given rule's `WHERE` clause isn't known
    /// generically — e.g. `owner_id`, `user_id`, `assignee_id`).
    ///
    /// This covers every example in the reference docs but is NOT correct
    /// for multi-column filters, ranges, joins, or any `WHERE` clause more
    /// complex than a single foreign-key equality check — those require a
    /// real SQL predicate evaluator, which is out of scope here. Rules
    /// with such filters should re-verify via [`Self::assign`] (a real
    /// Postgres round-trip) rather than trusting this method's result as
    /// authoritative for security-sensitive routing decisions.
    pub fn find_affected_buckets(
        &self,
        entity_type: &str,
        row_data: &serde_json::Value,
    ) -> Vec<BucketId> {
        let mut matched = Vec::new();
        for entry in self.cache.iter() {
            let (assignments, inserted_at) = entry.value();
            if inserted_at.elapsed() >= self.cache_ttl {
                continue;
            }
            for assignment in assignments {
                if !assignment.data_queries.contains_key(entity_type) {
                    continue;
                }
                if row_matches_assignment(row_data, assignment) {
                    matched.push(assignment.bucket_id.clone());
                }
            }
        }
        matched
    }

    /// Resolve one [`pes_core::SyncRule`] against `claims`. Returns `None`
    /// if the rule's parameter queries produce no matching row (the user is
    /// simply not authorized for this bucket — not an error condition).
    async fn resolve_rule(
        &self,
        rule: &pes_core::SyncRule,
        claims: &TokenClaims,
    ) -> Result<Option<BucketAssignment>, SyncError> {
        let mut resolved_parameters: HashMap<String, String> = HashMap::new();
        let mut resolved_json: HashMap<String, serde_json::Value> = HashMap::new();

        for param_name in &rule.parameters {
            let query = rule.parameter_queries.get(param_name).ok_or_else(|| {
                SyncError::BucketAssignmentFailed(format!(
                    "rule '{}' declares parameter '{param_name}' with no parameter_queries entry",
                    rule.id
                ))
            })?;

            // SECURITY: claims.sub is bound as $1 — never interpolated into
            // the query string. This is the only place user-controlled
            // input touches SQL, and it goes through sqlx's parameter
            // binding, which the Postgres wire protocol transmits
            // separately from the query text.
            let row = sqlx::query(query)
                .bind(&claims.sub)
                .fetch_optional(&self.pool)
                .await?;

            let Some(row) = row else {
                // No row resolved for this parameter — the user doesn't
                // qualify for this bucket. Not an error.
                return Ok(None);
            };

            let value: String = row
                .try_get::<String, _>(0)
                .or_else(|_| row.try_get::<i64, _>(0).map(|v| v.to_string()))
                .map_err(|e| {
                    SyncError::BucketAssignmentFailed(format!(
                        "rule '{}' parameter '{param_name}' query returned an unsupported column type: {e}",
                        rule.id
                    ))
                })?;

            if !template::validate_safe_value(&value) {
                return Err(SyncError::BucketAssignmentFailed(format!(
                    "rule '{}' parameter '{param_name}' resolved to a value that failed the safe-value allowlist",
                    rule.id
                )));
            }

            resolved_json.insert(param_name.clone(), serde_json::Value::String(value.clone()));
            resolved_parameters.insert(param_name.clone(), value);
        }

        let mut data_queries = HashMap::new();
        for (query_name, query_template) in &rule.data_queries {
            let rendered = template::substitute(&rule.id, query_template, &resolved_parameters)
                .map_err(|e| SyncError::BucketAssignmentFailed(e.to_string()))?;
            data_queries.insert(query_name.clone(), rendered);
        }

        Ok(Some(BucketAssignment {
            // SECURITY: `bucket_id` is the internal wire/oplog partition
            // key and MUST be unique per (rule, resolved parameter values)
            // — i.e. per authorized user, not per rule. `pes-router`
            // appends each changed row exactly once to the oplog partition
            // keyed by `bucket_id`, and `pes-gateway` streams a partition's
            // entire contents to every session subscribed to it with no
            // further per-session filtering. Using the bare rule id here
            // (as an earlier version of this code did) makes every user
            // matching the same rule share one physical partition, so any
            // two such users see each other's live deltas — a cross-tenant
            // data leak caught by `v4-pem-sync-transport`'s two-tab
            // different-users integration test. `rule_id` (below) remains
            // the bare, client-facing name clients `Subscribe` with and
            // that `pes-gateway` echoes back on the wire — see
            // `ConnectionHandler::handshake`'s match against `rule_id`,
            // not `bucket_id`.
            bucket_id: BucketId(format!("{}:{}", rule.id, parameter_key(&resolved_json))),
            rule_id: rule.id.clone(),
            parameters: resolved_json,
            data_queries,
        }))
    }
}

/// Build a stable, order-independent string key from a rule's resolved
/// parameter values, for use as the unique suffix of an internal
/// [`BucketId`]. Sorted by parameter name so the same resolved values
/// always produce the same key regardless of `HashMap` iteration order.
fn parameter_key(resolved: &HashMap<String, serde_json::Value>) -> String {
    let mut entries: Vec<(&String, &serde_json::Value)> = resolved.iter().collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));
    entries
        .into_iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join(",")
}

/// Single-owner-column heuristic used by [`BucketAssigner::find_affected_buckets`]:
/// does any of `assignment`'s resolved parameter values equal the value of
/// a same-named-ish column in `row_data`? Since the actual `WHERE` column
/// name a rule's `data_queries` filter uses isn't known generically, this
/// tries every resolved parameter value against every top-level scalar
/// field in `row_data` and matches on any equal pair.
fn row_matches_assignment(row_data: &serde_json::Value, assignment: &BucketAssignment) -> bool {
    let Some(row_object) = row_data.as_object() else {
        return false;
    };
    assignment.parameters.values().any(|param_value| {
        row_object
            .values()
            .any(|column_value| json_scalars_equal(param_value, column_value))
    })
}

/// Compare two JSON scalars for equality, tolerating a string/number type
/// mismatch (e.g. a resolved parameter stored as `"42"` matching a row
/// column that decoded as the JSON number `42`) — `BucketAssignment`
/// parameters are always stored as strings (see `resolve_rule` above),
/// while `row_to_json`-derived row data preserves Postgres's native
/// column types, so a strict `Value::eq` would never match a numeric id.
fn json_scalars_equal(a: &serde_json::Value, b: &serde_json::Value) -> bool {
    if a == b {
        return true;
    }
    match (a, b) {
        (serde_json::Value::String(s), serde_json::Value::Number(n))
        | (serde_json::Value::Number(n), serde_json::Value::String(s)) => *s == n.to_string(),
        _ => false,
    }
}
