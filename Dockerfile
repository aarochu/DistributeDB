FROM rust:1.92-slim-bookworm AS build
WORKDIR /source
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --offline --release --bin distributedb

FROM debian:bookworm-slim
RUN useradd --system --uid 10001 --create-home ddb \
    && mkdir /data \
    && chown ddb:ddb /data
COPY --from=build /source/target/release/distributedb /usr/local/bin/distributedb
USER ddb
EXPOSE 5555 5556
ENTRYPOINT ["distributedb"]
