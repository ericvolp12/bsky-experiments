CREATE TABLE author_blocks (
    actor_did TEXT NOT NULL,
    target_did TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (actor_did, target_did)
);
