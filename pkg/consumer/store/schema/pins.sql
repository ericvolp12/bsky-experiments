-- Pins
CREATE TABLE pins (
    actor_did TEXT NOT NULL,
    rkey TEXT NOT NULL
);
CREATE UNIQUE INDEX pins_pk ON pins (actor_did, rkey DESC);
