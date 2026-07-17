//! `ConnectionHandler` — per-connection lifecycle: auth, snapshot delivery,
//! delta subscription (polling), write handling, keepalive.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use futures::{SinkExt, StreamExt};
use pes_core::{BucketAssignment, BucketId, BucketOp, Op, PgLsn, SyncError};
use pes_oplog::BucketOpLog;
use pes_protocol::{decode_client, encode_server, ClientMessage, ServerMessage};
use pes_rules::BucketAssigner;
use pes_snapshot::SnapshotStream;
use sqlx::PgPool;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use tokio_util::sync::CancellationToken;

use crate::auth::JwtValidator;
use crate::error::GatewayErrorResponse;
use crate::server::GatewayConfig;

/// Wire error code for [`ServerMessage::Error`] sent to every connected
/// client when the server begins a graceful shutdown. Distinct from
/// [`crate::error::GatewayErrorCode`]'s `wire_code()` range (4001-4099) —
/// this isn't a redacted [`SyncError`], it's a server-lifecycle signal a
/// well-behaved client should treat as "reconnect later," not an auth or
/// protocol failure.
pub const SHUTDOWN_ERROR_CODE: u16 = 1001;

/// Drives one client connection's full lifecycle from the initial
/// `Subscribe` handshake through to disconnect.
pub struct ConnectionHandler {
    ws_stream: WebSocketStream<TcpStream>,
    config: Arc<GatewayConfig>,
    assigner: Arc<BucketAssigner>,
    oplog: Arc<BucketOpLog>,
    jwt_validator: Arc<JwtValidator>,
    write_pool: PgPool,
    shutdown: CancellationToken,
}

/// Per-connection state established after a successful `Subscribe`.
struct Session {
    authorized_buckets: HashMap<BucketId, BucketAssignment>,
    last_seen_lsn: HashMap<BucketId, PgLsn>,
}

impl ConnectionHandler {
    /// Construct a handler for an already-upgraded WebSocket connection.
    ///
    /// `shutdown` is a [`CancellationToken`] shared across every connection
    /// on the server — when the server begins a graceful shutdown, it
    /// cancels the parent token, and every `ConnectionHandler`'s `serve()`
    /// loop observes the cancellation, sends [`SHUTDOWN_ERROR_CODE`], and
    /// closes.
    pub fn new(
        ws_stream: WebSocketStream<TcpStream>,
        config: Arc<GatewayConfig>,
        assigner: Arc<BucketAssigner>,
        oplog: Arc<BucketOpLog>,
        jwt_validator: Arc<JwtValidator>,
        write_pool: PgPool,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            ws_stream,
            config,
            assigner,
            oplog,
            jwt_validator,
            write_pool,
            shutdown,
        }
    }

    /// Run this connection to completion: handshake, then the main
    /// send/receive/keepalive/delta-poll loop until disconnect.
    pub async fn run(mut self) -> Result<(), SyncError> {
        let session = match self.handshake().await {
            Ok(session) => session,
            Err(e) => {
                let response = GatewayErrorResponse::from(&e);
                let _ = self.send_error(response.code.wire_code(), &response.message).await;
                return Err(e);
            }
        };

        self.deliver_snapshots(&session).await?;
        self.serve(session).await
    }

    /// Wait for `Subscribe`, validate the JWT, and resolve authorized
    /// buckets (intersected with the client's requested buckets).
    async fn handshake(&mut self) -> Result<Session, SyncError> {
        let msg = self
            .ws_stream
            .next()
            .await
            .ok_or_else(|| SyncError::ProtocolError("connection closed before Subscribe".to_string()))?
            .map_err(|e| SyncError::ProtocolError(format!("WebSocket error: {e}")))?;

        let Message::Binary(bytes) = msg else {
            return Err(SyncError::ProtocolError(
                "expected a binary Subscribe frame".to_string(),
            ));
        };

        let client_msg = decode_client(&bytes)
            .map_err(|e| SyncError::ProtocolError(format!("malformed Subscribe: {e}")))?;

        let ClientMessage::Subscribe {
            buckets: requested_buckets,
            token,
            resume_lsn,
            protocol_version: _,
        } = client_msg
        else {
            return Err(SyncError::ProtocolError(
                "first message must be Subscribe".to_string(),
            ));
        };

        let claims = self.jwt_validator.validate(&token).await?;
        let assignments = self.assigner.assign(&claims).await?;

        // Clients request/see buckets by `rule_id` (the bare, config-level
        // name, e.g. "user_entities") — never `bucket_id`, which is the
        // internal, per-user-unique oplog partition key (see
        // `BucketAssigner::resolve_rule`'s doc comment on why the two must
        // differ). `authorized_buckets`/`last_seen_lsn` are still keyed by
        // the real `bucket_id` internally, so `poll_deltas`'s oplog scans
        // stay correctly scoped to just this session's own partition.
        let requested: HashSet<String> = requested_buckets.into_iter().collect();
        let mut authorized_buckets = HashMap::new();
        for assignment in assignments {
            if requested.contains(&assignment.rule_id) {
                authorized_buckets.insert(assignment.bucket_id.clone(), assignment);
            }
        }

        let mut last_seen_lsn = HashMap::new();
        if let Some(resume_lsn) = resume_lsn {
            for bucket_id in authorized_buckets.keys() {
                last_seen_lsn.insert(bucket_id.clone(), resume_lsn);
            }
        }

        Ok(Session {
            authorized_buckets,
            last_seen_lsn,
        })
    }

    /// For buckets with no `resume_lsn` (fresh subscriptions), stream the
    /// initial snapshot. Buckets resuming from a known LSN skip this and
    /// rely on `drain_since` in the main loop instead.
    async fn deliver_snapshots(&mut self, session: &Session) -> Result<(), SyncError> {
        let fresh_assignments: Vec<BucketAssignment> = session
            .authorized_buckets
            .iter()
            .filter(|(bucket_id, _)| !session.last_seen_lsn.contains_key(bucket_id))
            .map(|(_, assignment)| assignment.clone())
            .collect();

        if fresh_assignments.is_empty() {
            return Ok(());
        }

        for assignment in &fresh_assignments {
            // `bucket_id` (internal, per-user-unique) is used for every
            // oplog operation; `wire_bucket_id` (the bare, client-facing
            // rule name) is what actually goes out on the wire, matching
            // what the client `Subscribe`d with. See `handshake`'s comment
            // and `BucketAssigner::resolve_rule` for why these differ.
            let bucket_id = assignment.bucket_id.clone();
            let wire_bucket_id = BucketId(assignment.rule_id.clone());
            let checksum = self.oplog.checksum(&bucket_id).await?;
            let total_rows = 0u64; // Row count is only known after the stream completes; see SnapshotComplete.
            self.send_message(&ServerMessage::SnapshotBegin {
                bucket_id: wire_bucket_id.clone(),
                total_rows,
                protocol_version: pes_protocol::PROTOCOL_VERSION,
            })
            .await?;

            let stream = SnapshotStream::new(self.write_pool.clone(), vec![assignment.clone()]);
            let mut stream = Box::pin(stream.stream());
            let mut offset = 0u64;
            while let Some(batch) = stream.next().await {
                let batch = batch?;
                self.send_message(&ServerMessage::SnapshotBatch {
                    bucket_id: wire_bucket_id.clone(),
                    rows: batch.rows,
                    offset,
                })
                .await?;
                offset += 1;
            }

            self.send_message(&ServerMessage::SnapshotComplete {
                bucket_id: wire_bucket_id.clone(),
                checksum,
            })
            .await?;
        }

        Ok(())
    }

    /// Main loop: concurrently reads client frames, polls each authorized
    /// bucket's oplog for new deltas, and sends periodic keepalives — until
    /// the client disconnects or an unrecoverable error occurs.
    ///
    /// Errors from the background delta-poll and keepalive ticks (not
    /// directly triggered by a client message, so `handle_client_message`'s
    /// own error-response handling doesn't cover them) are still reported to
    /// the client via a `GatewayErrorResponse`-derived `Error` frame before
    /// the connection is torn down, per `GatewayErrorResponse`'s documented
    /// invariant that every response path must go through it.
    async fn serve(mut self, mut session: Session) -> Result<(), SyncError> {
        let mut keepalive = tokio::time::interval(self.config.keepalive_interval);
        let mut delta_poll = tokio::time::interval(self.config.delta_poll_interval);

        loop {
            tokio::select! {
                incoming = self.ws_stream.next() => {
                    let Some(incoming) = incoming else {
                        return Ok(());
                    };
                    let msg = incoming.map_err(|e| SyncError::ProtocolError(format!("WebSocket error: {e}")))?;
                    match msg {
                        Message::Binary(bytes) => {
                            if !self.handle_client_message(&bytes, &mut session).await? {
                                return Ok(());
                            }
                        }
                        Message::Close(_) => return Ok(()),
                        _ => {}
                    }
                }
                _ = delta_poll.tick() => {
                    if let Err(e) = self.poll_deltas(&mut session).await {
                        let response = GatewayErrorResponse::from(&e);
                        let _ = self.send_error(response.code.wire_code(), &response.message).await;
                        return Err(e);
                    }
                }
                _ = keepalive.tick() => {
                    self.send_message(&ServerMessage::Keepalive { server_time_ms: now_ms() }).await?;
                }
                () = self.shutdown.cancelled() => {
                    let _ = self.send_error(SHUTDOWN_ERROR_CODE, "server shutting down").await;
                    return Ok(());
                }
            }
        }
    }

    /// Handle one decoded client frame. Returns `Ok(false)` if the
    /// connection should close (currently unused but keeps `serve`'s loop
    /// exit conditions in one place).
    async fn handle_client_message(
        &mut self,
        bytes: &[u8],
        session: &mut Session,
    ) -> Result<bool, SyncError> {
        let client_msg = match decode_client(bytes) {
            Ok(msg) => msg,
            Err(e) => {
                let response = GatewayErrorResponse::from(&SyncError::ProtocolError(e.to_string()));
                self.send_error(response.code.wire_code(), &response.message).await?;
                return Ok(true);
            }
        };

        match client_msg {
            ClientMessage::Ack { lsn } => {
                for tracked in session.last_seen_lsn.values_mut() {
                    if *tracked < lsn {
                        *tracked = lsn;
                    }
                }
            }
            ClientMessage::Write {
                entity_type,
                entity_id,
                op,
            } => {
                self.handle_write(session, entity_type, entity_id, op).await?;
            }
            ClientMessage::Ping => {
                self.send_message(&ServerMessage::Keepalive { server_time_ms: now_ms() })
                    .await?;
            }
            ClientMessage::Subscribe { .. } => {
                let response = GatewayErrorResponse::from(&SyncError::ProtocolError(
                    "Subscribe may only be sent once, at connection start".to_string(),
                ));
                self.send_error(response.code.wire_code(), &response.message).await?;
            }
            // `ClientMessage` is #[non_exhaustive] for forward compatibility;
            // an unknown future variant is treated as a protocol error rather
            // than silently ignored.
            _ => {
                let response = GatewayErrorResponse::from(&SyncError::ProtocolError(
                    "unrecognized client message variant".to_string(),
                ));
                self.send_error(response.code.wire_code(), &response.message).await?;
            }
        }

        Ok(true)
    }

    /// Validate a write against an authorized bucket's entity type AND the
    /// specific `entity_id`'s row-level ownership, apply it, and let the
    /// existing WAL → `WalToBucketRouter` → oplog pipeline propagate it back
    /// out as a `Delta` (including to this same connection, via the normal
    /// poll loop) — this handler does not push the write's effect directly.
    ///
    /// # Authorization (both checks required)
    ///
    /// 1. **Entity type**: some authorized bucket's `data_queries` must have
    ///    a key matching `entity_type` (the same convention
    ///    `docs/sync-rules-reference.md` uses: a `data` query name is the
    ///    real table name, e.g. `entities`).
    /// 2. **Row ownership**: `entity_id` must fall within that assignment's
    ///    already-resolved, owner-scoped `data_queries` predicate — checked
    ///    by executing `SELECT 1 FROM (<data_query>) AS sq WHERE sq.id = $1`
    ///    against the *same* trusted query string `SnapshotStream` uses for
    ///    reads (see `pes_rules::BucketAssigner::resolve_rule`: the string
    ///    is built via allowlisted template substitution, never
    ///    client-controlled). Only `$1` — the bound `entity_id` — is
    ///    per-request untrusted input.
    ///
    /// Checking only (1) would let any client authorized to read/write *any*
    /// row of `entity_type` write, delete, or CRDT-patch *any other* row of
    /// that type — including rows owned by a different user/tenant — and
    /// have the unauthorized write re-broadcast as a legitimate-looking
    /// `Delta` to the real owner's other sessions via the WAL pipeline. This
    /// was flagged by a pre-archive security review and fixed here before
    /// this crate's tasks were considered complete.
    async fn handle_write(
        &mut self,
        session: &Session,
        entity_type: String,
        entity_id: String,
        op: Op,
    ) -> Result<(), SyncError> {
        let Some(assignment) = session
            .authorized_buckets
            .values()
            .find(|assignment| assignment.data_queries.contains_key(&entity_type))
        else {
            let response = GatewayErrorResponse::from(&SyncError::AuthError(format!(
                "not authorized to write entity type '{entity_type}'"
            )));
            self.send_error(response.code.wire_code(), &response.message).await?;
            return Ok(());
        };

        // `Delete` and `CrdtPatch`-of-an-existing-row both require the row
        // to already exist within the caller's owned scope. `Upsert` covers
        // both insert and update; for an insert of a brand-new id this
        // ownership check will correctly fail closed (the row doesn't exist
        // yet under any predicate) — callers must create new entities via
        // the ordinary application write path, not `ClientMessage::Write`,
        // until this protocol gains an explicit "create" semantics that can
        // establish initial ownership itself. This is a deliberate scope
        // limit, not an oversight: silently trusting a client-asserted
        // owner value on insert would reopen the exact hole this fix closes.
        let data_query = &assignment.data_queries[&entity_type];
        let owns_row = self.row_is_within_scope(data_query, &entity_id).await?;
        if !owns_row {
            let response = GatewayErrorResponse::from(&SyncError::AuthError(format!(
                "not authorized to write entity '{entity_type}/{entity_id}'"
            )));
            self.send_error(response.code.wire_code(), &response.message).await?;
            return Ok(());
        }

        let result = self.apply_write(&entity_type, &entity_id, op).await;
        if let Err(e) = &result {
            let response = GatewayErrorResponse::from(e);
            self.send_error(response.code.wire_code(), &response.message).await?;
            return Ok(());
        }
        result
    }

    /// Does `entity_id` fall within `data_query`'s already owner-scoped
    /// result set? `data_query` is always a trusted, server-resolved string
    /// (see `handle_write`'s doc comment) — only the bound `entity_id` is
    /// per-request untrusted input.
    async fn row_is_within_scope(&self, data_query: &str, entity_id: &str) -> Result<bool, SyncError> {
        let wrapped = format!("SELECT 1 FROM ({data_query}) AS sq WHERE sq.id::text = $1 LIMIT 1");
        let row: Option<i32> = sqlx::query_scalar(&wrapped)
            .bind(entity_id)
            .fetch_optional(&self.write_pool)
            .await?;
        Ok(row.is_some())
    }

    /// Apply an already-authorized write. `entity_type` is used directly as
    /// the target table name — see `handle_write`'s doc comment on the
    /// `data` query name / table name convention.
    ///
    /// SECURITY: `entity_type` is interpolated via `format!` into each SQL
    /// statement below, which would be a SQL injection risk for arbitrary
    /// client input — but by the time `apply_write` is called,
    /// `handle_write` has already confirmed `entity_type` is an exact match
    /// for a key in some authorized `BucketAssignment.data_queries`, a
    /// finite, server-controlled set defined entirely by the deployed
    /// `sync-rules.toml` (see `pes_rules::parser`/`docs/sync-rules-reference.md`).
    /// A client cannot cause `entity_type` to take any value outside that
    /// operator-defined set — `handle_write`'s `HashMap::contains_key`
    /// lookup fails closed for anything else, before this function is ever
    /// reached. `entity_id` (the only remaining per-request, genuinely
    /// client-controlled input to these queries) is always bound via `$1`,
    /// never interpolated.
    async fn apply_write(&self, entity_type: &str, entity_id: &str, op: Op) -> Result<(), SyncError> {
        match op {
            Op::Upsert(value) => {
                let sql = format!(
                    "UPDATE {entity_type} SET payload = $1 WHERE id::text = $2",
                );
                sqlx::query(&sql)
                    .bind(&value)
                    .bind(entity_id)
                    .execute(&self.write_pool)
                    .await?;
            }
            Op::Delete => {
                let sql = format!("DELETE FROM {entity_type} WHERE id::text = $1");
                sqlx::query(&sql).bind(entity_id).execute(&self.write_pool).await?;
            }
            Op::CrdtPatch(bytes) => {
                let select_sql = format!("SELECT crdt_state FROM {entity_type} WHERE id::text = $1");
                let existing: Option<Vec<u8>> = sqlx::query_scalar(&select_sql)
                    .bind(entity_id)
                    .fetch_optional(&self.write_pool)
                    .await?;
                let merged = frf_crdt::apply_delta(existing.as_deref().unwrap_or(&[]), &bytes)
                    .map_err(|e| SyncError::ProtocolError(format!("CRDT patch apply failed: {e}")))?;
                let update_sql = format!("UPDATE {entity_type} SET crdt_state = $1 WHERE id::text = $2");
                sqlx::query(&update_sql)
                    .bind(&merged)
                    .bind(entity_id)
                    .execute(&self.write_pool)
                    .await?;
            }
            // `Op` is #[non_exhaustive]; an unknown future variant can't be
            // applied safely, so reject it rather than silently no-op.
            _ => {
                return Err(SyncError::ProtocolError(
                    "unrecognized Op variant".to_string(),
                ));
            }
        }

        Ok(())
    }

    /// Poll each authorized bucket's oplog for ops past `last_seen_lsn` and
    /// push them as `Delta` messages.
    ///
    /// This is polling, not push, because `BucketOpLog::drain_since` is a
    /// one-shot range scan with no live-notification mechanism (no
    /// `tokio::sync::broadcast` or similar). See the `v4-pes-gateway`
    /// design discussion: extending `pes-oplog` with a subscribe API was
    /// judged out of scope for this change, since that crate is already
    /// shipped and stable.
    async fn poll_deltas(&mut self, session: &mut Session) -> Result<(), SyncError> {
        let bucket_ids: Vec<BucketId> = session.authorized_buckets.keys().cloned().collect();
        for bucket_id in bucket_ids {
            // `last_seen_lsn` tracks the highest LSN already delivered to
            // this client (inclusive) — what a `ClientMessage::Ack` reports
            // and what `Delta.lsn` below echoes back. `drain_since`'s scan
            // start, however, is documented as inclusive of `from_lsn`, so
            // scanning from the same value stored here would re-include
            // the already-delivered op on every subsequent poll forever
            // (verified live: without the +1 offset below, a single write
            // was redelivered on every 50ms poll tick indefinitely). The
            // scan start is therefore one past the last delivered LSN,
            // kept as a local-only value distinct from `last_seen_lsn`.
            let last_delivered = session.last_seen_lsn.get(&bucket_id).copied();
            let scan_from = last_delivered.map(|lsn| PgLsn(lsn.0 + 1)).unwrap_or(PgLsn(0));

            let mut stream = Box::pin(self.oplog.drain_since(&bucket_id, scan_from));
            let mut ops: Vec<BucketOp> = Vec::new();
            while let Some(op) = stream.next().await {
                ops.push(op?);
            }

            if ops.is_empty() {
                continue;
            }

            let max_lsn = ops.iter().map(|op| op.lsn).max().unwrap_or(scan_from);
            // Rewrite each op's internal, per-user-unique `bucket_id` to
            // the bare, client-facing rule name before it goes on the wire
            // — otherwise the internal partition key (which embeds this
            // session's own resolved parameter values, e.g. an internal
            // user_id) would leak into every delta frame, even though no
            // current client actually reads `BucketOp.bucket_id`.
            let wire_bucket_id = session
                .authorized_buckets
                .get(&bucket_id)
                .map(|assignment| BucketId(assignment.rule_id.clone()))
                .unwrap_or_else(|| bucket_id.clone());
            let ops: Vec<BucketOp> = ops
                .into_iter()
                .map(|op| BucketOp {
                    bucket_id: wire_bucket_id.clone(),
                    ..op
                })
                .collect();
            self.send_message(&ServerMessage::Delta {
                bucket_id: wire_bucket_id,
                ops,
                lsn: max_lsn,
            })
            .await?;
            session.last_seen_lsn.insert(bucket_id, max_lsn);
        }
        Ok(())
    }

    async fn send_message(&mut self, msg: &ServerMessage) -> Result<(), SyncError> {
        let bytes = encode_server(msg).map_err(|e| SyncError::ProtocolError(e.to_string()))?;
        self.ws_stream
            .send(Message::Binary(bytes.into()))
            .await
            .map_err(|e| SyncError::ProtocolError(format!("WebSocket send failed: {e}")))
    }

    async fn send_error(&mut self, code: u16, message: &str) -> Result<(), SyncError> {
        self.send_message(&ServerMessage::Error {
            code,
            message: message.to_string(),
        })
        .await
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
