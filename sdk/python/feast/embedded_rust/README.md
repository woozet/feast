Embedded Rust (cffi)

This directory contains a CFFI-based Python wrapper that calls a Rust shared
library built from `feast/rust`.

Build (developer)
- `cd feast/rust`
- `cargo build --release`
- copy the shared library into `feast/sdk/python/feast/embedded_rust/lib/`
  - macOS: `libfeast_rust.dylib`
  - Linux: `libfeast_rust.so`
  - Windows: `feast_rust.dll`

You can also set `FEAST_RUST_LIB_PATH` to point at the shared library.
