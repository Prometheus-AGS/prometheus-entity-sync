# pes-server — multi-stage build: rust:1.94-slim builder -> debian:bookworm-slim runtime.
#
# Builder version note: the proposal specified rust:1.87-slim, but a real
# `docker build` run (not just trusting the proposal) found several
# dependencies (redb 4.1.0, time/time-core/time-macros, home) with an MSRV
# above 1.87 — 1.87 fails to compile the workspace. Bumped to 1.94-slim.
#
# Runtime base note: the proposal specified gcr.io/distroless/cc, but
# pg_walstream (a frf-postgres-cdc dependency) dynamically links libpq,
# whose own transitive closure is ~15 shared libraries (GSSAPI, Kerberos,
# LDAP, SASL, etc.) — distroless has no package manager, so manually
# curating and copying that whole chain is fragile and, worse, gives no way
# to `apt upgrade` a CVE'd libssl/libpq/libkrb5 inside the running image.
# debian:bookworm-slim + `apt-get install libpq5` is the standard,
# maintainable pattern for a libpq-linked Rust binary; confirmed via a real
# build that the final image is still far under the 100MB budget.
#
# BUILD CONTEXT: this Dockerfile must be built with a context that is the
# PARENT of this repo, containing both `prometheus-entity-sync/` and
# `flint-realtime-fabric/` as sibling directories — matching the workspace's
# `[workspace.dependencies]` relative `path = "../flint-realtime-fabric/..."`
# entries (see Cargo.toml). Build from the parent directory:
#
#   cd /path/to/parent
#   docker build -f prometheus-entity-sync/Dockerfile -t pes-server .
#
# (examples/docker-compose/docker-compose.yml's build.context is set
# accordingly — see that file.)
#
# IGNORE FILES: the parent build-context directory is shared with other,
# unrelated projects — writing a context-root `.dockerignore` there would
# affect their builds too. Instead this repo ships
# `Dockerfile.dockerignore` (BuildKit's per-Dockerfile ignore-file
# convention: `<dockerfile>.dockerignore`, resolved relative to the
# Dockerfile's own path when `-f` is used), which excludes both sibling
# repos' `target/` directories from the build context. Requires BuildKit
# (`DOCKER_BUILDKIT=1`, on by default in modern `docker build`/`docker
# buildx`).

FROM rust:1.94-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    ca-certificates \
    libpq-dev \
    libclang-dev \
    clang \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy both sibling workspaces referenced by prometheus-entity-sync's
# path-based [workspace.dependencies].
COPY flint-realtime-fabric/ ./flint-realtime-fabric/
COPY prometheus-entity-sync/ ./prometheus-entity-sync/

WORKDIR /build/prometheus-entity-sync

RUN cargo build --release --bin pes-server

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --no-create-home --uid 10001 pes-server \
    && mkdir -p /var/lib/pes-server/oplog \
    && chown -R pes-server:pes-server /var/lib/pes-server

COPY --from=builder /build/prometheus-entity-sync/target/release/pes-server /usr/local/bin/pes-server

USER pes-server

# WebSocket gateway port (config.toml's [server].port) and health/metrics
# port (config.toml's [metrics].port) — both configurable, these EXPOSE
# lines document the config.toml defaults, not a hard requirement.
EXPOSE 8080 9090

ENTRYPOINT ["/usr/local/bin/pes-server"]
