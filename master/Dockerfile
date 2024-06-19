FROM rust:latest

WORKDIR /usr/src/myapp

COPY . .

COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

RUN cargo build --release

ENTRYPOINT ["entrypoint.sh"]
