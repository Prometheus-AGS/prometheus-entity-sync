//! Compile-time assertions that public types have the trait bounds required
//! to be held behind `Arc` and shared across the async gateway.

use pes_rules::SyncRuleSet;

#[test]
fn sync_rule_set_is_clone_send_sync() {
    fn assert_bounds<T: Clone + Send + Sync>() {}
    assert_bounds::<SyncRuleSet>();
}
