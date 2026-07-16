//! Semantic validation of a parsed [`SyncRuleSet`].

use std::sync::LazyLock;

use regex::Regex;

use crate::error::ParseError;
use crate::parser::SyncRuleSet;

static BUCKET_ID_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[a-z][a-z0-9_-]*$").expect("bucket id regex is valid"));

/// Matches `{bucket_parameters.NAME}` references inside a data query string.
static BUCKET_PARAM_REF_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\{bucket_parameters\.([A-Za-z0-9_]+)\}").expect("regex is valid"));

/// Matches any SQL positional placeholder, e.g. `$1`, `$2`, `$10`. Used to
/// find placeholders other than the single allowed `$1`.
static PLACEHOLDER_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\$([0-9]+)").expect("regex is valid"));

/// Validate a parsed [`SyncRuleSet`] against the sync-rules semantic rules:
///
/// - Every name in `parameters` has a matching entry in `parameter_queries`.
/// - Every `parameter_queries` value contains exactly one placeholder, `$1`,
///   and no other `$N` placeholders.
/// - Every `{bucket_parameters.X}` reference in a data query matches a
///   declared parameter name.
/// - Bucket ids match `[a-z][a-z0-9_-]*`.
///
/// Circular bucket references are not currently representable in the DSL —
/// data queries may only reference this bucket's own `bucket_parameters`,
/// not other buckets — so there is nothing to detect a cycle in yet. This
/// function still returns `Ok(())` for that rule; see `docs/sync-rules-reference.md`.
pub fn validate(rule_set: &SyncRuleSet) -> Result<(), ParseError> {
    for (bucket_id, rule) in &rule_set.rules {
        validate_bucket_id(bucket_id)?;
        validate_parameters_have_queries(bucket_id, rule)?;
        validate_parameter_query_placeholders(bucket_id, rule)?;
        validate_data_query_references(bucket_id, rule)?;
    }
    Ok(())
}

fn validate_bucket_id(bucket_id: &str) -> Result<(), ParseError> {
    if BUCKET_ID_RE.is_match(bucket_id) {
        Ok(())
    } else {
        Err(ParseError::Validation {
            bucket_id: bucket_id.to_string(),
            message: format!(
                "bucket id '{bucket_id}' must match [a-z][a-z0-9_-]*"
            ),
        })
    }
}

fn validate_parameters_have_queries(
    bucket_id: &str,
    rule: &pes_core::SyncRule,
) -> Result<(), ParseError> {
    for param in &rule.parameters {
        if !rule.parameter_queries.contains_key(param) {
            return Err(ParseError::Validation {
                bucket_id: bucket_id.to_string(),
                message: format!(
                    "parameter '{param}' has no corresponding entry in parameter_queries"
                ),
            });
        }
    }
    Ok(())
}

fn validate_parameter_query_placeholders(
    bucket_id: &str,
    rule: &pes_core::SyncRule,
) -> Result<(), ParseError> {
    for (param, query) in &rule.parameter_queries {
        let placeholders: Vec<&str> = PLACEHOLDER_RE
            .captures_iter(query)
            .map(|cap| cap.get(1).expect("group 1 always matches").as_str())
            .collect();
        let only_dollar_one = !placeholders.is_empty() && placeholders.iter().all(|n| *n == "1");
        if !only_dollar_one {
            return Err(ParseError::Validation {
                bucket_id: bucket_id.to_string(),
                message: format!(
                    "parameter_queries.{param} must contain exactly the placeholder $1 and no other $N placeholders"
                ),
            });
        }
    }
    Ok(())
}

fn validate_data_query_references(
    bucket_id: &str,
    rule: &pes_core::SyncRule,
) -> Result<(), ParseError> {
    for (query_name, query) in &rule.data_queries {
        for cap in BUCKET_PARAM_REF_RE.captures_iter(query) {
            let referenced = &cap[1];
            if !rule.parameters.iter().any(|p| p == referenced) {
                return Err(ParseError::Validation {
                    bucket_id: bucket_id.to_string(),
                    message: format!(
                        "data.{query_name} references undeclared parameter 'bucket_parameters.{referenced}'"
                    ),
                });
            }
        }
    }
    Ok(())
}
