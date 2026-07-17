//! Safe template substitution for `{bucket_parameters.X}` references in data queries.
//!
//! # Security
//!
//! This is the last line of defense against SQL injection via JWT claims.
//! Parameter values originate from `parameter_queries` results — themselves
//! resolved via fully parameterized SQL (`$1` bound to the JWT `sub`) — but
//! a compromised or misconfigured Postgres row could still return an
//! attacker-controlled string. The allowlist regex below is therefore
//! enforced unconditionally on every substitution value, with no bypass.

use std::sync::LazyLock;

use regex::Regex;

use crate::error::ParseError;

/// Values substituted into `{bucket_parameters.X}` must match this pattern:
/// identifiers only (UUIDs, integers, slugs). Anything else — quotes,
/// semicolons, whitespace, SQL keywords, null bytes — is rejected.
static SAFE_VALUE_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[a-zA-Z0-9_-]{1,128}$").expect("safe value regex is valid"));

/// Matches `{bucket_parameters.NAME}` references inside a data query string.
static TEMPLATE_REF_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\{bucket_parameters\.([A-Za-z0-9_]+)\}").expect("template ref regex is valid")
});

/// Validate that `value` is safe to substitute into a SQL query string.
///
/// Accepts UUIDs, integers, and slug-like identifiers matching
/// `^[a-zA-Z0-9_-]{1,128}$`. Rejects everything else, including empty
/// strings, whitespace, quotes, and SQL metacharacters.
pub fn validate_safe_value(value: &str) -> bool {
    SAFE_VALUE_RE.is_match(value)
}

/// Substitute every `{bucket_parameters.X}` reference in `query` with the
/// corresponding value from `resolved_parameters`, rendered as a single-quoted
/// SQL string literal (e.g. `'abc-123'`).
///
/// Every substituted value is always rendered as text, never a bare token —
/// this is what makes the substitution valid SQL regardless of the target
/// column's type. A rule author comparing against a numeric column must cast
/// explicitly on the query side, e.g.
/// `SELECT * FROM t WHERE count > {bucket_parameters.min_count}::int`.
/// This is safe unconditionally because [`validate_safe_value`]'s allowlist
/// (`^[a-zA-Z0-9_-]{1,128}$`) already excludes quote characters, so no value
/// can escape the literal it's wrapped in.
///
/// Returns [`ParseError::Validation`] if any referenced parameter is
/// missing from `resolved_parameters`, or if its value fails
/// [`validate_safe_value`]. No substitution happens on error — the
/// original query is never partially rendered.
pub fn substitute(
    bucket_id: &str,
    query: &str,
    resolved_parameters: &std::collections::HashMap<String, String>,
) -> Result<String, ParseError> {
    // First pass: validate every reference before rendering anything, so a
    // late invalid parameter can't leave an earlier valid substitution in
    // a partially-rendered, inconsistent query string.
    for cap in TEMPLATE_REF_RE.captures_iter(query) {
        let name = &cap[1];
        let value = resolved_parameters.get(name).ok_or_else(|| ParseError::Validation {
            bucket_id: bucket_id.to_string(),
            message: format!("template references unresolved parameter 'bucket_parameters.{name}'"),
        })?;
        if !validate_safe_value(value) {
            return Err(ParseError::Validation {
                bucket_id: bucket_id.to_string(),
                message: format!(
                    "value for parameter '{name}' failed the safe-value allowlist (must match [a-zA-Z0-9_-]{{1,128}})"
                ),
            });
        }
    }

    let rendered = TEMPLATE_REF_RE
        .replace_all(query, |caps: &regex::Captures<'_>| {
            let name = &caps[1];
            // Safe: presence and validity were already confirmed above, and
            // the allowlist excludes `'`, so this can't break out of the
            // literal it's wrapped in.
            let value = resolved_parameters.get(name).cloned().unwrap_or_default();
            format!("'{value}'")
        })
        .into_owned();

    Ok(rendered)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn valid_uuid_passes() {
        assert!(validate_safe_value("550e8400-e29b-41d4-a716-446655440000"));
    }

    #[test]
    fn valid_integer_passes() {
        assert!(validate_safe_value("12345"));
    }

    #[test]
    fn valid_slug_passes() {
        assert!(validate_safe_value("user_42-alpha"));
    }

    #[test]
    fn sql_injection_payload_rejected() {
        assert!(!validate_safe_value("'; DROP TABLE users; --"));
    }

    #[test]
    fn empty_string_rejected() {
        assert!(!validate_safe_value(""));
    }

    #[test]
    fn whitespace_rejected() {
        assert!(!validate_safe_value("has space"));
    }

    #[test]
    fn null_byte_rejected() {
        assert!(!validate_safe_value("abc\0def"));
    }

    #[test]
    fn overlong_value_rejected() {
        let too_long = "a".repeat(129);
        assert!(!validate_safe_value(&too_long));
    }

    #[test]
    fn max_length_value_accepted() {
        let max_len = "a".repeat(128);
        assert!(validate_safe_value(&max_len));
    }

    #[test]
    fn substitute_renders_valid_reference() {
        let mut params = HashMap::new();
        params.insert("user_id".to_string(), "abc-123".to_string());
        let rendered = substitute(
            "b1",
            "SELECT * FROM entities WHERE owner_id = {bucket_parameters.user_id}",
            &params,
        )
        .expect("should substitute");
        assert_eq!(rendered, "SELECT * FROM entities WHERE owner_id = 'abc-123'");
    }

    #[test]
    fn substitute_quotes_uuid_value_for_valid_sql() {
        let mut params = HashMap::new();
        params.insert(
            "user_id".to_string(),
            "550e8400-e29b-41d4-a716-446655440000".to_string(),
        );
        let rendered = substitute(
            "b1",
            "SELECT * FROM entities WHERE owner_id = {bucket_parameters.user_id}",
            &params,
        )
        .expect("should substitute");
        assert_eq!(
            rendered,
            "SELECT * FROM entities WHERE owner_id = '550e8400-e29b-41d4-a716-446655440000'"
        );
    }

    #[test]
    fn substitute_rejects_unresolved_parameter() {
        let params = HashMap::new();
        let err = substitute(
            "b1",
            "SELECT * FROM entities WHERE owner_id = {bucket_parameters.user_id}",
            &params,
        )
        .unwrap_err();
        assert!(matches!(err, ParseError::Validation { .. }));
    }

    #[test]
    fn substitute_rejects_unsafe_value() {
        let mut params = HashMap::new();
        params.insert("user_id".to_string(), "'; DROP TABLE users; --".to_string());
        let err = substitute(
            "b1",
            "SELECT * FROM entities WHERE owner_id = {bucket_parameters.user_id}",
            &params,
        )
        .unwrap_err();
        assert!(matches!(err, ParseError::Validation { .. }));
    }

    #[test]
    fn substitute_with_no_references_returns_query_unchanged() {
        let params = HashMap::new();
        let rendered = substitute("b1", "SELECT * FROM countries", &params).expect("no refs");
        assert_eq!(rendered, "SELECT * FROM countries");
    }

    #[test]
    fn substitute_does_not_partially_render_on_error() {
        let mut params = HashMap::new();
        params.insert("user_id".to_string(), "abc-123".to_string());
        // tenant_id is unresolved — the whole substitution must fail, even
        // though user_id would have rendered fine.
        let err = substitute(
            "b1",
            "SELECT * FROM t WHERE owner_id = {bucket_parameters.user_id} AND tenant_id = {bucket_parameters.tenant_id}",
            &params,
        )
        .unwrap_err();
        assert!(matches!(err, ParseError::Validation { .. }));
    }
}
