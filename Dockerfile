FROM rust:1.93.1 AS chef
WORKDIR /app
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true
RUN cargo install cargo-chef --version 0.1.77

# Build frontend
FROM node:22-slim AS frontend-builder
WORKDIR /app/frontend
COPY frontend/package*.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
ENV SQLX_OFFLINE=true
RUN cargo build --release

FROM debian:bookworm-slim AS runtime
RUN apt-get update -y \
    && apt-get install -y --no-install-recommends openssl ca-certificates \
    # Clean up
    && apt-get autoremove -y \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/acurast-indexer /usr/local/bin/acurast-indexer
COPY --from=frontend-builder /app/frontend/dist ./frontend/dist
COPY ./migrations ./migrations
COPY ./configuration ./configuration
# set ENVIRONMENT to make dotenvy pick up the production config file
CMD ["acurast-indexer"]
