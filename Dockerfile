FROM rust:1.91.1-bookworm AS build
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim
COPY --from=build /build/target/release/shopping-sessions /usr/local/bin/shopping-sessions
USER 1000:1000
ENV DATA_DIR=/data OUTPUT_DIR=/output NCPU=8
ENTRYPOINT ["/usr/local/bin/shopping-sessions"]
