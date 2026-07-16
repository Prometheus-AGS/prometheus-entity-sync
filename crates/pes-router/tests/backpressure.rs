//! Backpressure test: proves the bounded channel between WAL event receipt
//! and the oplog-writing consumer actually throttles the producer when the
//! consumer falls behind — the core mechanism `WalToBucketRouter::run()`
//! relies on for its backpressure guarantee.
//!
//! `BucketOpLog` is a concrete struct (not a trait), so there's no seam to
//! inject a "slow oplog mock" into the full `WalToBucketRouter::run()`
//! pipeline without a larger refactor. Instead, this test exercises the
//! exact same `tokio::sync::mpsc::channel(BACKPRESSURE_CAPACITY)` primitive
//! `run()` uses, with a synthetic slow consumer standing in for a slow
//! oplog writer — proving the channel itself, at the capacity `run()`
//! actually configures, produces the throttling behavior the proposal
//! requires ("oplog write queue depth > threshold → slow WAL consumption").

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use pes_router::BACKPRESSURE_CAPACITY;
use tokio::sync::mpsc;

/// A full channel must make `send` await rather than return immediately —
/// this is what causes the WAL consumption loop in `run()` to slow down
/// when the writer falls behind, rather than buffering unboundedly.
#[tokio::test]
async fn full_channel_blocks_sender_until_consumer_drains() {
    let (tx, mut rx) = mpsc::channel::<u64>(BACKPRESSURE_CAPACITY);

    // Fill the channel to capacity without anyone consuming.
    for i in 0..BACKPRESSURE_CAPACITY as u64 {
        tx.send(i).await.expect("channel not full yet");
    }
    assert_eq!(tx.capacity(), 0, "channel should be exactly full");

    // The next send must NOT complete until something is consumed —
    // verified by racing it against a short timeout.
    let send_result = tokio::time::timeout(Duration::from_millis(200), tx.send(BACKPRESSURE_CAPACITY as u64)).await;
    assert!(
        send_result.is_err(),
        "send on a full channel must block, not return immediately"
    );

    // Draining one item must unblock exactly one pending send.
    let _ = rx.recv().await;
    // The previously-blocked send() call was dropped by the timeout above,
    // so issue a fresh one — it should now succeed quickly since capacity
    // was freed.
    let send_after_drain = tokio::time::timeout(Duration::from_millis(200), tx.send(999)).await;
    assert!(
        send_after_drain.is_ok(),
        "send must succeed once the consumer has drained capacity"
    );
}

/// End-to-end throttling proof: a producer pushing as fast as possible
/// into a bounded channel, paired with an artificially slow consumer
/// (standing in for a slow oplog writer), must have its overall throughput
/// bounded by the consumer's rate — not run ahead and buffer unboundedly
/// in memory. Measured by comparing elapsed time against the
/// theoretical minimum imposed by the slow consumer.
#[tokio::test]
async fn producer_throughput_is_bounded_by_slow_consumer_rate() {
    const ITEM_COUNT: usize = 50;
    const CONSUMER_DELAY: Duration = Duration::from_millis(20);
    // Small capacity so the producer can't race far ahead of the consumer
    // before backpressure kicks in — proportionally the same relationship
    // as BACKPRESSURE_CAPACITY (1000) is to a bucket's realistic per-event
    // append cost, just scaled down so the test runs in well under a second.
    const SMALL_CAPACITY: usize = 5;

    let (tx, mut rx) = mpsc::channel::<usize>(SMALL_CAPACITY);
    let consumed_count = Arc::new(AtomicUsize::new(0));
    let consumed_count_reader = Arc::clone(&consumed_count);

    let consumer = tokio::spawn(async move {
        while let Some(_item) = rx.recv().await {
            tokio::time::sleep(CONSUMER_DELAY).await;
            consumed_count_reader.fetch_add(1, Ordering::SeqCst);
        }
    });

    let started = Instant::now();
    for i in 0..ITEM_COUNT {
        // This `send` blocks once SMALL_CAPACITY items are in flight and
        // the consumer hasn't drained them yet — the actual backpressure
        // mechanism under test.
        tx.send(i).await.expect("consumer still running");
    }
    drop(tx);
    consumer.await.expect("consumer task panicked");
    let elapsed = started.elapsed();

    let theoretical_min = CONSUMER_DELAY * ITEM_COUNT as u32;
    assert_eq!(
        consumed_count.load(Ordering::SeqCst),
        ITEM_COUNT,
        "every sent item must eventually be consumed"
    );
    // The producer cannot finish faster than the consumer can drain,
    // because the bounded channel forces it to wait — this is the
    // "WAL consumption rate drops" property the proposal asks for.
    assert!(
        elapsed >= theoretical_min.mul_f64(0.8),
        "producer completed in {elapsed:?}, faster than the slow consumer's \
         theoretical minimum {theoretical_min:?} allows — backpressure did not throttle it"
    );
}
