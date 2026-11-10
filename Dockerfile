# syntax=docker/dockerfile:1
FROM rust:1.93-bookworm AS build
WORKDIR /build
RUN rustup target add wasm32-unknown-unknown
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY examples ./examples
COPY wit ./wit
COPY scripts ./scripts
# Build inside Linux; do not copy the host's macOS binaries or target directory.
ARG CARGO_BUILD_JOBS=6
ENV CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS}
RUN cargo build --locked --release -p loom-cli -p loom-conformance -p loom-sort-client --bin loom --bin hold-claim --bin loom-sort-client
RUN target/release/loom build
RUN cargo build --locked --release -p loom-example-sort --target wasm32-unknown-unknown \
    && target/release/loom component target/wasm32-unknown-unknown/release/loom_example_sort.wasm target/sort.component.wasm
RUN cargo test --locked --release --workspace --no-run

FROM build AS tests
ENV LOOM_TEST_COMPONENT=/build/target/research-agent.component.wasm
CMD ["sh", "scripts/container_tests.sh"]

FROM debian:bookworm-slim AS runtime
COPY --from=build /build/target/release/loom /usr/local/bin/loom
COPY --from=build /build/target/release/hold-claim /usr/local/bin/hold-claim
COPY --from=build /build/target/release/loom-sort-client /usr/local/bin/loom-sort-client
COPY --from=build /build/target/research-agent.component.wasm /opt/loom/research-agent.component.wasm
COPY --from=build /build/target/sort.component.wasm /opt/loom/sort.component.wasm
USER 10001:10001
WORKDIR /opt/loom
ENTRYPOINT ["/usr/local/bin/loom"]
CMD ["--help"]
