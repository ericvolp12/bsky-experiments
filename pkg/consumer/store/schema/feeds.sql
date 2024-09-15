-- Pins
CREATE TABLE pins (
    actor_did TEXT NOT NULL,
    rkey TEXT NOT NULL
);
CREATE UNIQUE INDEX pins_pk ON pins (actor_did, rkey DESC);
-- Label Feeds
CREATE TABLE actor_labels (
    actor_did TEXT NOT NULL,
    label TEXT NOT NULL,
    PRIMARY KEY (actor_did, label)
);
CREATE TABLE mpls (
    actor_did TEXT NOT NULL,
    rkey TEXT NOT NULL,
    PRIMARY KEY (actor_did, rkey)
);
CREATE INDEX mpls_paging_idx ON mpls (rkey DESC);
