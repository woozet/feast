# Rust feature server (WIP)

Early Rust implementation targeting parity with the Go feature server.

## Features
- HTTP and gRPC `GetOnlineFeatures`
- File-based registry loading (`registry_store_type: file`)
- Redis online store (node/cluster)
- OnDemand Feature View transformations via gRPC transformation service
- Entity-less (dummy entity) handling

## Build and run
```bash
cd feast/rust
cargo build --release
./target/release/feast-rust --type=http --port=8080 --chdir /path/to/feature_repo
# or gRPC
# ./target/release/feast-rust --type=grpc --port=8080 --chdir /path/to/feature_repo
```

The server reads `feature_store.yaml` from the repository path (`--chdir`, default `.`).

## Embedded Rust (cffi)
There is a CFFI-based wrapper under `sdk/python/feast/embedded_rust/` that can
load the Rust shared library for embedded online serving experiments.

## Configuration
Minimal `feature_store.yaml`:
```yaml
project: my_project
registry:
  registry_store_type: file
  path: data/registry.db
online_store:
  type: redis
  connection_string: localhost:6379
```

Optional transformation service:
```yaml
feature_server:
  transformation_service_endpoint: 127.0.0.1:6566
```

## HTTP API
`POST /get-online-features`
```bash
curl -X POST "http://localhost:8080/get-online-features?status=true" \
  -H "Content-Type: application/json" \
  -d '{
    "features": ["driver_stats:rating"],
    "entities": {"driver_id": [1001]},
    "full_feature_names": false
  }'
```

## Tests
```bash
cd feast/rust
cargo test
```

Optional integration tests:
```bash
FEAST_REDIS_TESTS=1 FEAST_REDIS_ADDR=localhost:6379 cargo test
FEAST_TRANSFORM_TESTS=1 cargo test --test odfv_transformation_integration
```

## Notes
- HTTP JSON output matches Go Arrow JSON for supported Feast value types.
- Transformation service and Redis cluster behavior are supported but lightly tested.
- See `feast/rust/DEV_GUIDE.md` for current status and planned work.
