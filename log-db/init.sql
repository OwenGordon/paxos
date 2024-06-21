CREATE DATABASE logs;
\c logs;

CREATE TABLE logs (
    id SERIAL PRIMARY KEY,
    timestamp VARCHAR,
    source VARCHAR,
    destination VARCHAR,
    action VARCHAR
);

CREATE INDEX idx_items_timestamp ON logs (timestamp);
CREATE INDEX idx_items_source ON logs (source);
CREATE INDEX idx_items_destination ON logs (destination);
CREATE INDEX idx_items_action ON logs (action);