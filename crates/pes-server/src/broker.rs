//! A minimal, dependency-free in-process `LogBroker` — an `mpsc` channel
//! dressed up behind the `LogBroker` trait, bridging `PostgresCdcConsumer`
//! (the producer) to `WalToBucketRouter` (the single consumer) within one
//! `pes-server` process.
//!
//! This is deliberately NOT `frf-broker-iggy`'s `IggyBroker`: that requires
//! a live Iggy server, external infrastructure this binary's proposed
//! docker-compose deployment (server + Postgres only) never provisions.
//! For a single-instance deployment — the only topology this binary
//! currently supports — an in-process channel is a correct, minimal bridge:
//! WAL events never need to leave this process or be shared across
//! `pes-server` replicas. A horizontally-scaled multi-instance deployment
//! would need a real distributed broker (Iggy or otherwise); that's future
//! scope, not something this binary's initial docker-compose target
//! requires.

use std::sync::Mutex;

use async_trait::async_trait;
use frf_domain::{Channel, ChannelId, Cursor, EventEnvelope, Offset};
use frf_ports::{EventStream, LogBroker, PortError};
use tokio_stream::wrappers::ReceiverStream;

const CHANNEL_BUFFER: usize = 1024;

/// See module docs.
pub struct InProcessBroker {
    tx: Mutex<Option<tokio::sync::mpsc::Sender<Result<EventEnvelope, PortError>>>>,
    rx: Mutex<Option<tokio::sync::mpsc::Receiver<Result<EventEnvelope, PortError>>>>,
    next_offset: Mutex<u64>,
}

impl InProcessBroker {
    pub fn new() -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(CHANNEL_BUFFER);
        Self {
            tx: Mutex::new(Some(tx)),
            rx: Mutex::new(Some(rx)),
            next_offset: Mutex::new(0),
        }
    }
}

impl Default for InProcessBroker {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl LogBroker for InProcessBroker {
    async fn publish(&self, mut envelope: EventEnvelope) -> Result<Offset, PortError> {
        let offset = {
            let mut next = self.next_offset.lock().unwrap();
            let o = Offset(*next);
            *next += 1;
            o
        };
        envelope.offset = offset;
        let tx = self.tx.lock().unwrap().clone();
        if let Some(tx) = tx {
            tx.send(Ok(envelope))
                .await
                .map_err(|e| PortError::Transport(e.to_string()))?;
        }
        Ok(offset)
    }

    async fn subscribe(
        &self,
        _channel_id: ChannelId,
        _consumer_id: String,
        _from: Offset,
    ) -> Result<EventStream, PortError> {
        let rx = self
            .rx
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| PortError::Transport("InProcessBroker::subscribe called more than once".to_string()))?;
        Ok(Box::pin(ReceiverStream::new(rx)))
    }

    async fn seek(&self, _cursor: Cursor) -> Result<(), PortError> {
        Ok(())
    }

    async fn ack(&self, _channel_id: ChannelId, _consumer_id: &str, _offset: Offset) -> Result<(), PortError> {
        Ok(())
    }

    async fn ensure_channel(&self, _channel: Channel) -> Result<(), PortError> {
        Ok(())
    }
}
