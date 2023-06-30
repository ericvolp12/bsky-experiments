CREATE TABLE post_labels (
    post_id TEXT NOT NULL,
    author_did TEXT NOT NULL,
    label TEXT NOT NULL,
    PRIMARY KEY (post_id, author_did, label),
    FOREIGN KEY (author_did) REFERENCES authors(did),
    FOREIGN KEY (post_id) REFERENCES posts(id)
);
