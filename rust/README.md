# Rust feature server (WIP)

This is an early Rust implementation targeting parity with the Go feature server.

## Build and run

```bash
cargo build --release
./target/release/feast-rust --type=http --port=8080
# or gRPC
# ./target/release/feast-rust --type=grpc --port=8080
```

The server reads `feature_store.yaml` from the repository path (`--chdir`, default `.`).
