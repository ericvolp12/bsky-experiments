-- Pins
CREATE TABLE pins (
    actor_did TEXT NOT NULL,
    rkey TEXT NOT NULL
);
CREATE UNIQUE INDEX pins_pk ON pins (actor_did, rkey DESC);
-- Actor Label Feeds
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
CREATE TABLE tqsp (
    actor_did TEXT NOT NULL,
    rkey TEXT NOT NULL,
    PRIMARY KEY (actor_did, rkey)
);
CREATE INDEX tqsp_paging_idx ON tqsp (rkey DESC);
-- Post Label Feeds
CREATE TABLE recent_post_labels (
    actor_did TEXT NOT NULL,
    rkey TEXT NOT NULL,
    label TEXT NOT NULL,
    subject_id BIGINT,
    PRIMARY KEY (actor_did, rkey, label)
);
CREATE INDEX recent_post_labels_label_idx ON recent_post_labels (label);
CREATE INDEX recent_post_labels_paging_idx ON recent_post_labels (rkey DESC);
