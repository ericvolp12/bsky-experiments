CREATE TABLE authors (
    did TEXT PRIMARY KEY,
    handle TEXT NOT NULL,
    cluster_opt_out BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE TABLE clusters (
    id SERIAL PRIMARY KEY,
    lookup_alias TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL
);
CREATE TABLE author_clusters (
    author_did TEXT NOT NULL,
    cluster_id INT NOT NULL,
    PRIMARY KEY (author_did),
    FOREIGN KEY (author_did) REFERENCES authors(did),
    FOREIGN KEY (cluster_id) REFERENCES clusters(id)
);
CREATE TABLE posts (
    id TEXT PRIMARY KEY,
    text TEXT NOT NULL,
    parent_post_id TEXT,
    root_post_id TEXT,
    author_did TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    has_embedded_media BOOLEAN NOT NULL,
    parent_relationship CHAR(3),
    sentiment CHAR(3),
    sentiment_confidence FLOAT,
    indexed_at TIMESTAMPTZ,
    FOREIGN KEY (author_did) REFERENCES authors(did)
);
CREATE table images (
    cid TEXT NOT NULL,
    post_id TEXT NOT NULL,
    author_did TEXT NOT NULL,
    alt_text TEXT,
    mime_type TEXT NOT NULL,
    fullsize_url TEXT NOT NULL,
    thumbnail_url TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    cv_completed BOOLEAN NOT NULL,
    cv_run_at TIMESTAMPTZ,
    cv_classes JSONB,
    PRIMARY KEY (cid, post_id),
    FOREIGN KEY (post_id) REFERENCES posts (id),
    FOREIGN KEY (author_did) REFERENCES authors (did)
);
CREATE TABLE post_labels (
    post_id TEXT NOT NULL,
    author_did TEXT NOT NULL,
    label TEXT NOT NULL,
    PRIMARY KEY (post_id, author_did, label),
    FOREIGN KEY (author_did) REFERENCES authors(did),
    FOREIGN KEY (post_id) REFERENCES posts(id)
);
CREATE TABLE post_likes (
    post_id TEXT NOT NULL,
    author_did TEXT,
    like_count BIGINT NOT NULL,
    PRIMARY KEY (post_id)
);
CREATE TABLE author_blocks (
    actor_did TEXT NOT NULL,
    target_did TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (actor_did, target_did)
);
CREATE TABLE labels (
    id BIGSERIAL PRIMARY KEY,
    lookup_alias TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL
);
CREATE TABLE author_labels (
    author_did TEXT NOT NULL,
    label_id INT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (author_did, label_id),
    FOREIGN KEY (author_did) REFERENCES authors(did),
    FOREIGN KEY (label_id) REFERENCES labels(id)
);
