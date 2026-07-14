FROM rust:1.96-bookworm AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY migrations ./migrations
RUN cargo build --locked --release

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system chathygiene \
    && useradd --system --gid chathygiene --no-create-home \
        --home-dir /nonexistent --shell /usr/sbin/nologin chathygiene \
    && mkdir /data \
    && chown chathygiene:chathygiene /data

COPY --from=builder --chown=chathygiene:chathygiene \
    /app/target/release/chathygiene /usr/local/bin/chathygiene

ENV CHATHYGIENE_DATABASE_URL=sqlite:///data/chathygiene.db \
    CHATHYGIENE_DESTRUCTIVE_MODE=false \
    RUST_LOG=info

USER chathygiene
EXPOSE 8080
VOLUME ["/data"]

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl --fail --silent http://127.0.0.1:8080/health/ready || exit 1

ENTRYPOINT ["/usr/local/bin/chathygiene"]
