#!/bin/sh
set -eu
export CARGO_NET_OFFLINE=true
cargo test --locked --release --workspace
cargo test --locked --release -p loom-conformance --lib -- --ignored --nocapture
target/release/loom demo target/research-agent.component.wasm
