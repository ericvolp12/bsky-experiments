CREATE TABLE posts (
    id TEXT PRIMARY KEY,
    text TEXT NOT NULL,
    parent_post_id TEXT,
    root_post_id TEXT,
    author_did TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    has_embedded_media BOOLEAN NOT NULL,
    parent_relationship CHAR(3),
    FOREIGN KEY (author_did) REFERENCES authors(did)
);
