# Rust Feature Server Dev Guide

This file captures the current Rust feature server work so it can be resumed quickly.

## Current status
- Rust crate lives under `feast/rust`.
- Feature store config loading, registry file loading, Redis online store keying, and HTTP/gRPC serving are implemented.
- HTTP endpoint `/get-online-features` and gRPC `GetOnlineFeatures` are wired to the Rust pipeline.
- OnDemand Feature Views are supported via the transformation service (Arrow IPC).
- Entity-less (dummy entity) handling is supported in `FeatureStore`.
- Tests: `cargo test` passes (requires network access the first time to fetch crates). Integration tests include a registry fixture and an optional Redis roundtrip.

## Implemented pieces
- Config parsing: `feature_store.yaml` with env expansion in `feast/rust/src/config.rs`.
- Registry: file-based registry proto loading in `feast/rust/src/registry.rs`.
- Model mapping: `Entity`, `FeatureView`, `FeatureService`, projections in `feast/rust/src/model.rs`.
- Online serving logic: grouping, join key validation, TTL check, feature vector assembly in `feast/rust/src/onlineserving.rs`.
- Redis online read: HMGET with Go-compatible keys and field hashing in `feast/rust/src/onlinestore.rs`.
- Transformation service: gRPC client + Arrow IPC request/response handling in `feast/rust/src/transformation.rs`.
- HTTP/gRPC servers: `feast/rust/src/server/http.rs`, `feast/rust/src/server/grpc.rs`.
- Entry point: `feast/rust/src/main.rs`.
- Tests: `feast/rust/tests/odfv_selection.rs` covers ODFV selection logic for feature services and feature refs.

## Known gaps / TODO
- OnDemand Feature Views (ODFV) selection has unit tests, but end-to-end integration coverage with transformation service is still limited.
- Feature logging (feature service logging_config) is not implemented.
- HTTP response format matches Go Arrow JSON for supported Feast value types.
- Redis cluster behavior is untested (cluster feature enabled, no `ReadOnly` tuning yet).
- Redis integration test requires `FEAST_REDIS_TESTS=1` and a local Redis instance.

## Next steps (recommended order)
1) Add tests for OnDemand Feature View + transformation service integration.
2) Align HTTP JSON response format with Go (Arrow-like JSON) if strict parity is required.
3) Add feature logging support when feature service has logging_config.
4) Add Redis cluster integration tests and tuning as needed.

## PR readiness checklist (suggested)
- `cargo test` passes.
- Redis integration tests pass (`FEAST_REDIS_TESTS=1` with local Redis).
- ODFV transformation integration test passes (`FEAST_TRANSFORM_TESTS=1`).
- Python CFFI smoke test validates `EmbeddedRustOnlineFeatureServer`.
- Docs mention any known gaps (feature logging, type limits, cluster support).

## ODFV end-to-end test placement
- Put E2E ODFV tests under `feast/rust/tests/`, e.g. `odfv_e2e.rs`.
- Keep them behind an opt-in env var (`FEAST_PY_TRANSFORM_TESTS=1`) because they require Python + Feast deps.
- Run the Python transformation server in a subprocess using the Feast Python API:
  - `FeatureStore(...).serve_transformations(port)` (see `feast/sdk/python/feast/transformation_server.py`).
  - The Rust test points `transformation_service_endpoint` at `http://127.0.0.1:<port>`.
- Recommended repo: `feast-compat-sample/feature_repo` (run `feast apply` to generate `data/registry.db`).

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

Optional Redis integration test:
```bash
cd feast/rust
FEAST_REDIS_TESTS=1 FEAST_REDIS_ADDR=localhost:6379 cargo test
```

Optional transformation service integration test:
```bash
cd feast/rust
FEAST_TRANSFORM_TESTS=1 cargo test --test odfv_transformation_integration
```

Optional Python transformation server E2E test (proposed):
```bash
cd feast/rust
FEAST_PY_TRANSFORM_TESTS=1 \
FEAST_PY_TRANSFORM_REPO=../../feast-compat-sample/feature_repo \
FEAST_PY_ODFV_NAME=transformed_conv_rate \
cargo test --test odfv_e2e
```

Optional CFFI smoke test:
```bash
cd feast/rust
FEAST_CFFI_TESTS=1 \
FEAST_CFFI_REPO=../../feast-compat-sample/feature_repo \
FEAST_RUST_LIB_PATH=../sdk/python/feast/embedded_rust/lib/libfeast_rust.dylib \
cargo test --test cffi_e2e
```

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
- `feast/rust/src/server/http.rs` (HTTP endpoint)
- `feast/rust/src/server/grpc.rs` (gRPC endpoint)
- `feast/rust/src/encoding.rs` (JSON/proto conversions)
- `feast/rust/src/featurestore.rs` (orchestration)
