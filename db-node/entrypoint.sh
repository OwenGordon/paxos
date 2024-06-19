#!/bin/sh

redis-server --daemonize yes

./target/release/db-node
