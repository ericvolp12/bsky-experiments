CREATE TABLE posts (
    actor_did TEXT NOT NULL,
    rkey TEXT NOT NULL,
    content TEXT,
    parent_post_actor_did TEXT,
    parent_post_rkey TEXT,
    parent_relationship TEXT,
    root_post_actor_did TEXT,
    root_post_rkey TEXT,
    has_embedded_media BOOLEAN DEFAULT FALSE NOT NULL,
    created_at TIMESTAMPTZ,
    inserted_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    PRIMARY KEY (actor_did, rkey)
);
CREATE INDEX posts_inserted_at ON posts (inserted_at DESC);
CREATE TABLE likes (
    actor_did TEXT NOT NULL,
    rkey TEXT NOT NULL,
    subject_actor_did TEXT NOT NULL,
    subject_namespace TEXT NOT NULL,
    subject_rkey TEXT NOT NULL,
    created_at TIMESTAMPTZ,
    inserted_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    PRIMARY KEY (actor_did, rkey)
);
CREATE TABLE like_counts (
    actor_did TEXT NOT NULL,
    ns TEXT NOT NULL,
    rkey TEXT NOT NULL,
    num_likes BIGINT NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    PRIMARY KEY (actor_did, ns, rkey)
);
CREATE INDEX like_counts_count ON like_counts (num_likes DESC);
CREATE TABLE blocks (
    actor_did TEXT NOT NULL,
    rkey TEXT NOT NULL,
    target_did TEXT NOT NULL,
    created_at TIMESTAMPTZ,
    inserted_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    PRIMARY KEY (actor_did, rkey)
);
CREATE TABLE follows (
    actor_did TEXT NOT NULL,
    rkey TEXT NOT NULL,
    target_did TEXT NOT NULL,
    created_at TIMESTAMPTZ,
    inserted_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    PRIMARY KEY (actor_did, rkey)
);
CREATE TABLE images (
    cid TEXT NOT NULL,
    post_actor_did TEXT NOT NULL,
    post_rkey TEXT NOT NULL,
    alt_text TEXT,
    created_at TIMESTAMPTZ,
    inserted_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    PRIMARY KEY (post_actor_did, post_rkey, cid)
);
CREATE FUNCTION update_updated_at() RETURNS TRIGGER AS $$ BEGIN NEW.updated_at = now();
RETURN NEW;
END;
$$ language 'plpgsql';
CREATE TRIGGER update_like_count_updated_at BEFORE
UPDATE ON like_counts FOR EACH ROW EXECUTE PROCEDURE update_updated_at();
CREATE MATERIALIZED VIEW recent_posts_with_score AS
SELECT p.actor_did,
    p.rkey,
    p.created_at,
    p.inserted_at,
    (
        (COALESCE(lc.num_likes, 0) - 1) / (
            EXTRACT(
                EPOCH
                FROM now() - p.inserted_at
            ) / 3600 + 2
        ) ^ 1.8
    )::float AS score
FROM posts p
    LEFT JOIN like_counts lc ON p.actor_did = lc.actor_did
    AND p.rkey = lc.rkey
WHERE p.inserted_at > (
        (NOW() - INTERVAL '5 minutes') - INTERVAL '24 hours'
    )
    AND p.parent_post_rkey is NULL
    AND p.root_post_rkey is NULL
    AND lc.num_likes > 10
    AND lc.updated_at > (NOW() - INTERVAL '24 hours');
CREATE INDEX recent_posts_with_score_score ON recent_posts_with_score (score DESC);
CREATE INDEX recent_posts_with_score_actor_rkey ON recent_posts_with_score (actor_did, rkey);