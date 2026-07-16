//! Sync rule TOML DSL: parsing, validation, and the bucket assigner.
#![warn(missing_docs)]

mod assigner;
mod error;
mod parser;
mod template;
mod validator;

pub use assigner::BucketAssigner;
pub use error::ParseError;
pub use parser::{parse_sync_rules, parse_sync_rules_str, SyncRuleSet};
pub use template::validate_safe_value;
pub use validator::validate;
