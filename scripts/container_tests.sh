#!/bin/sh
set -eu
export CARGO_NET_OFFLINE=true
cargo test --locked --release --workspace
cargo test --locked --release -p loom-conformance --lib -- --ignored --nocapture
cargo test --locked --release -p loom-conformance --test shuffle -- --ignored --nocapture
cargo test --locked --release -p loom-conformance --test object_store -- --ignored --nocapture
cargo test --locked --release -p loom-conformance --test postgres_rows -- --ignored --nocapture
target/release/loom demo target/research-agent.component.wasm
