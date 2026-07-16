//! TOML deserialization for `sync-rules.toml` into a [`SyncRuleSet`].

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use pes_core::SyncRule;
use serde::Deserialize;

use crate::error::ParseError;

/// A fully parsed set of sync rules, keyed by bucket id.
#[derive(Debug, Clone)]
pub struct SyncRuleSet {
    /// The `sync-rules.toml` schema version.
    pub version: String,
    /// All rules in this set, keyed by bucket id (the TOML table key under `[buckets.*]`).
    pub rules: HashMap<String, SyncRule>,
}

/// Raw TOML document shape for `sync-rules.toml`.
#[derive(Debug, Deserialize)]
struct RawDocument {
    #[serde(default = "default_version")]
    version: String,
    #[serde(default)]
    buckets: HashMap<String, RawBucket>,
}

fn default_version() -> String {
    "1".to_string()
}

/// Raw TOML shape for one `[buckets.<id>]` table.
#[derive(Debug, Deserialize)]
struct RawBucket {
    description: Option<String>,
    #[serde(default)]
    parameters: Vec<String>,
    #[serde(default)]
    parameter_queries: HashMap<String, String>,
    #[serde(default)]
    data: HashMap<String, String>,
}

/// Parse a `sync-rules.toml` file at `path` into a [`SyncRuleSet`].
///
/// This performs syntactic TOML parsing and shape mapping only; semantic
/// validation (parameter/query consistency, bucket id format, etc.) is
/// performed separately by [`crate::validator::validate`].
pub fn parse_sync_rules(path: &Path) -> Result<SyncRuleSet, ParseError> {
    let contents = fs::read_to_string(path).map_err(|source| ParseError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    parse_sync_rules_str(&contents)
}

/// Parse `sync-rules.toml` contents already loaded into memory.
pub fn parse_sync_rules_str(contents: &str) -> Result<SyncRuleSet, ParseError> {
    let raw: RawDocument = toml::from_str(contents)
        .map_err(|err| ParseError::from_toml_error(err, contents))?;

    let rules = raw
        .buckets
        .into_iter()
        .map(|(id, bucket)| {
            let rule = SyncRule {
                id: id.clone(),
                description: bucket.description,
                parameters: bucket.parameters,
                parameter_queries: bucket.parameter_queries,
                data_queries: bucket.data,
            };
            (id, rule)
        })
        .collect();

    Ok(SyncRuleSet {
        version: raw.version,
        rules,
    })
}
