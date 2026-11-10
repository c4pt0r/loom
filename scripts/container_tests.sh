#!/bin/sh
set -eu
export CARGO_NET_OFFLINE=true
cargo test --locked --release --workspace
cargo test --locked --release -p loom-conformance --lib -- --ignored --nocapture
cargo test --locked --release -p loom-conformance --test shuffle -- --ignored --nocapture
cargo test --locked --release -p loom-conformance --test object_store -- --ignored --nocapture
cargo test --locked --release -p loom-conformance --test postgres_rows -- --ignored --nocapture
cargo test --locked --release -p loom-conformance --test multi_actor -- --ignored --nocapture
cargo test --locked --release -p loom-conformance --test inspection -- --ignored --nocapture
target/release/loom demo target/research-agent.component.wasm

if [ "${LOOM_OBJECT_STORE:-postgres}" = rustfs ] || [ "${LOOM_OBJECT_STORE:-postgres}" = s3 ]; then
    cargo test --locked --release -p loom-object-store-s3 --test s3 -- --ignored
    cargo test --locked --release -p loom-conformance --test s3 -- --ignored
fi
