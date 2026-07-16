//! Sync rule TOML DSL: parsing, validation, and the bucket assigner.
#![warn(missing_docs)]

mod error;
mod parser;
mod validator;

pub use error::ParseError;
pub use parser::{parse_sync_rules, parse_sync_rules_str, SyncRuleSet};
pub use validator::validate;
