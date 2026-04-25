# syntax=docker/dockerfile:1

FROM rust:1-slim-bookworm AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends libprotobuf-dev protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY Cargo.toml Cargo.lock ./

RUN mkdir -p src \
    && printf 'fn main() {}\n' > src/main.rs \
    && printf '\n' > src/lib.rs \
    && cargo fetch --locked \
    && rm -rf src

COPY src src
COPY tests tests

RUN for lance_lib in $(find /usr/local/cargo/registry/src -path '*/lance*/src/lib.rs'); do \
        if grep -q '^#!\[recursion_limit = ' "$lance_lib"; then \
            sed -i 's/^#!\[recursion_limit = ".*"\]/#![recursion_limit = "1024"]/' "$lance_lib"; \
        else \
            sed -i '1i #![recursion_limit = "1024"]' "$lance_lib"; \
        fi; \
        head -n 1 "$lance_lib"; \
    done

RUN cargo build --locked

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /data/lancedb

COPY --from=builder /app/target/debug/engram /usr/local/bin/engram

ENV LANCE_DB_PATH=/data/lancedb
ENV ENGRAM_BIND_ADDR=0.0.0.0:3000

EXPOSE 3000

CMD ["engram"]