FROM rust:1.91.1-bookworm AS build
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
# The scoring machines are Cascade Lake or newer.
ENV RUSTFLAGS="-C target-cpu=x86-64-v3"
# Collect branch frequencies on freshly generated input. The final image
# receives only the optimized binary.
RUN set -eu; \
    if rustup component add llvm-tools-preview && \
       curl --retry 1 --max-time 25 -fsSL https://www.joinbuilders.org/buildathon/tracks/shopping-sessions/files/generator.zip -o /tmp/generator.zip && \
       echo '13724cadbac5a99d847e27cea911cd2e338ff972b46c2039da87452011bb101e  /tmp/generator.zip' | sha256sum -c - && \
       unzip -q /tmp/generator.zip -d /tmp; then \
        cargo run --release --offline --manifest-path /tmp/generator/Cargo.toml --bin generate -- --seed 78123 --events 400000 --clicks 100000 --threads 4 --output /tmp/training; \
        cargo rustc --release --locked -- -Cprofile-generate=/tmp/profiles; \
        DATA_DIR=/tmp/training OUTPUT_DIR=/tmp/train-out NCPU=4 target/release/shopping-sessions; \
        "$(rustc --print sysroot)/lib/rustlib/x86_64-unknown-linux-gnu/bin/llvm-profdata" merge -o /tmp/solver.profdata /tmp/profiles; \
        cargo rustc --release --locked -- -Cprofile-use=/tmp/solver.profdata; \
    else \
        echo 'Training tools unavailable; using the standard optimized build' >&2; \
        cargo build --release --locked; \
    fi

FROM debian:bookworm-slim
COPY --from=build /build/target/release/shopping-sessions /usr/local/bin/shopping-sessions
USER 1000:1000
ENV DATA_DIR=/data OUTPUT_DIR=/output NCPU=8
ENTRYPOINT ["/usr/local/bin/shopping-sessions"]
