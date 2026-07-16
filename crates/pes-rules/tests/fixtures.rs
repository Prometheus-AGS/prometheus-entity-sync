//! Integration tests: every fixture under `tests/fixtures/valid/` must parse
//! and validate successfully; every fixture under `tests/fixtures/invalid/`
//! must fail parsing or validation.

use std::fs;
use std::path::Path;

use pes_rules::{parse_sync_rules_str, validate};

fn fixture_paths(dir: &str) -> Vec<std::path::PathBuf> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(dir);
    let mut paths: Vec<_> = fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", dir.display()))
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|p| p.extension().is_some_and(|ext| ext == "toml"))
        .collect();
    paths.sort();
    paths
}

#[test]
fn at_least_ten_valid_fixtures_exist() {
    let paths = fixture_paths("valid");
    assert!(
        paths.len() >= 10,
        "expected at least 10 valid fixtures, found {}",
        paths.len()
    );
}

#[test]
fn at_least_ten_invalid_fixtures_exist() {
    let paths = fixture_paths("invalid");
    assert!(
        paths.len() >= 10,
        "expected at least 10 invalid fixtures, found {}",
        paths.len()
    );
}

#[test]
fn every_valid_fixture_parses_and_validates() {
    for path in fixture_paths("valid") {
        let contents = fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));
        let rule_set = parse_sync_rules_str(&contents)
            .unwrap_or_else(|e| panic!("{} should parse but failed: {e}", path.display()));
        validate(&rule_set)
            .unwrap_or_else(|e| panic!("{} should validate but failed: {e}", path.display()));
    }
}

#[test]
fn every_invalid_fixture_fails_parsing_or_validation() {
    for path in fixture_paths("invalid") {
        let contents = fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));
        match parse_sync_rules_str(&contents) {
            Err(_) => {} // syntax error — expected failure
            Ok(rule_set) => {
                assert!(
                    validate(&rule_set).is_err(),
                    "{} should fail parsing or validation but succeeded",
                    path.display()
                );
            }
        }
    }
}
