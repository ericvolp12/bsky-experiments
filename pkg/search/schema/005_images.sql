CREATE table images (
    cid TEXT NOT NULL,
    post_id TEXT NOT NULL,
    author_did TEXT NOT NULL,
    alt_text TEXT,
    mime_type TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    cv_completed BOOLEAN NOT NULL,
    cv_run_at TIMESTAMPTZ,
    cv_classes JSONB,
    PRIMARY KEY (cid, post_id),
    FOREIGN KEY (post_id) REFERENCES posts (id),
    FOREIGN KEY (author_did) REFERENCES authors (did)
);
