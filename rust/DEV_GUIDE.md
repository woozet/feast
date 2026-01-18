# Rust Feature Server Dev Guide

This file captures the current Rust feature server work so it can be resumed quickly.

## Current status
- Rust crate lives under `feast/rust`.
- Feature store config loading, registry file loading, Redis online store keying, and HTTP/gRPC serving are implemented.
- HTTP endpoint `/get-online-features` and gRPC `GetOnlineFeatures` are wired to the Rust pipeline.
- Tests: `cargo test` passes (requires network access the first time to fetch crates).

## Implemented pieces
- Config parsing: `feature_store.yaml` with env expansion in `feast/rust/src/config.rs`.
- Registry: file-based registry proto loading in `feast/rust/src/registry.rs`.
- Model mapping: `Entity`, `FeatureView`, `FeatureService`, projections in `feast/rust/src/model.rs`.
- Online serving logic: grouping, join key validation, TTL check, feature vector assembly in `feast/rust/src/onlineserving.rs`.
- Redis online read: HMGET with Go-compatible keys and field hashing in `feast/rust/src/onlinestore.rs`.
- HTTP/gRPC servers: `feast/rust/src/server.rs`.
- Entry point: `feast/rust/src/main.rs`.

## Known gaps / TODO
- OnDemand Feature Views (ODFV) and transformation service are not implemented.
- Feature logging (feature service logging_config) is not implemented.
- HTTP response format uses direct proto Value -> JSON conversion; Go uses Arrow JSON marshalling.
- Redis cluster behavior is untested (cluster feature enabled, no `ReadOnly` tuning yet).
- No integration tests yet (only unit tests for keying logic).
- Entity-less (dummy entity) handling is not implemented in Rust `FeatureStore`.

## Next steps (recommended order)
1) Add entity-less handling (dummy entity injection) to `FeatureStore::get_online_features`.
2) Add OnDemand Feature View support via transformation service (GrpcTransformationService equivalent).
3) Align HTTP JSON response format with Go (Arrow-like JSON) if strict parity is required.
4) Add integration tests (Redis + registry fixture) and a sample feature repo.
5) Add feature logging support when feature service has logging_config.

## How to build
```bash
cd feast/rust
cargo build
```

## How to test
```bash
cd feast/rust
cargo test
```
Note: first run may need network to download crates.

## How to run (HTTP)
```bash
cd feast/rust
cargo run -- --type=http --port=8080 --chdir /path/to/feature_repo
```

## How to run (gRPC)
```bash
cd feast/rust
cargo run -- --type=grpc --port=8080 --chdir /path/to/feature_repo
```

## HTTP API behavior
- `POST /get-online-features`
- Optional query: `?status=true` to include `statuses` and `event_timestamps`.

Example request:
```bash
curl -X POST "http://localhost:8080/get-online-features?status=true" \
  -H "Content-Type: application/json" \
  -d '{
    "features": ["driver_stats:conv_rate"],
    "entities": {"driver_id": [1001]},
    "full_feature_names": false
  }'
```

## Redis connection string
Rust uses the same `connection_string` format as Go:
- `host:port` (default)
- `host:port,password=secret,ssl=true,db=1`
- Multiple hosts are supported for cluster (`redis_type: redis_cluster`).

## Registry config expectations
`feature_store.yaml` must include a `registry` field with a local file path:
```yaml
registry:
  registry_store_type: file
  path: data/registry.db
```

## Git workflow (fork already exists)
```bash
git remote add fork git@github.com:woozet/feast.git
git checkout -b feat/rust-feature-server
git push -u fork feat/rust-feature-server
```

## Files to know
- `feast/rust/src/onlineserving.rs` (core request pipeline)
- `feast/rust/src/onlinestore.rs` (Redis keying + HMGET)
- `feast/rust/src/server.rs` (HTTP + gRPC endpoints)
- `feast/rust/src/featurestore.rs` (orchestration)

