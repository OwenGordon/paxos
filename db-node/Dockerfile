FROM rust:latest

RUN apt-get update && apt-get install -y redis-server dnsutils net-tools

WORKDIR /usr/src/myapp

COPY . .

COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

RUN cargo build --release

ENTRYPOINT ["entrypoint.sh"]
