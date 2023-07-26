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
