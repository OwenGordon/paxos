FROM rust:latest

WORKDIR /usr/src/myapp

COPY src /usr/src/myapp/src
COPY Cargo.toml /usr/src/myapp/Cargo.toml

COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

RUN cargo build --release

ENTRYPOINT ["entrypoint.sh"]
