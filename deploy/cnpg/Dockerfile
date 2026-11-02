# Build from the repository root:
# docker build --platform linux/arm64 -f deploy/cnpg/Dockerfile -t REGISTRY/pg-cache:17.11-0.1.0 .
FROM postgres:17.11-bookworm AS build
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential clang libclang-dev libssl-dev pkg-config \
    postgresql-server-dev-17 curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs -o /tmp/rustup.sh \
    && sh /tmp/rustup.sh -y --profile minimal --default-toolchain 1.92.0 \
    && rm /tmp/rustup.sh
ENV PATH="/root/.cargo/bin:${PATH}"
WORKDIR /src
COPY Cargo.toml Cargo.lock Makefile ./
COPY crates ./crates
RUN make install FEATURES=journal-prototype \
    PG_CONFIG=/usr/lib/postgresql/17/bin/pg_config DESTDIR=/stage

FROM ghcr.io/cloudnative-pg/postgresql:17.11-202610260819-system-bookworm@sha256:9cf9466e674a794aee83693b9c28d8b3103d612a943bf144117a718b97b9d1db
COPY --from=build /stage/ /
USER 26
