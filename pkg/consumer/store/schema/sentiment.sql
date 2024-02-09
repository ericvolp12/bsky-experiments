CREATE TABLE post_sentiments (
    actor_did TEXT NOT NULL,
    rkey TEXT NOT NULL,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ,
    sentiment TEXT,
    confidence FLOAT,
    detected_langs TEXT [],
    PRIMARY KEY (actor_did, rkey)
);
CREATE INDEX post_sentiments_actor_did_created_at_idx ON post_sentiments (actor_did, created_at DESC);
CREATE INDEX post_sentiments_unindexed_idx ON post_sentiments (created_at ASC)
WHERE processed_at IS NULL;
