FROM rust:1.96-alpine3.24 AS builder

RUN apk add --no-cache musl-dev pkgconfig

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY migrations ./migrations
RUN cargo build --locked --release

FROM alpine:3.24 AS runtime

RUN apk add --no-cache ca-certificates curl \
    && mkdir -m 0700 /data

COPY --from=builder \
    /app/target/release/chathygiene /usr/local/bin/chathygiene
COPY --chmod=0755 docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

ENV CHATHYGIENE_DATABASE_URL=sqlite:///data/chathygiene.db \
    CHATHYGIENE_DESTRUCTIVE_MODE=false \
    RUST_LOG=info

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=3s --start-period=10m --retries=3 \
    CMD curl --fail --silent http://127.0.0.1:8080/health/live || exit 1

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["chathygiene"]
