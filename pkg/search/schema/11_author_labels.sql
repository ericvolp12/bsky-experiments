CREATE TABLE author_labels (
    author_did TEXT NOT NULL,
    label_id INT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (author_did, label_id),
    FOREIGN KEY (author_did) REFERENCES authors(did),
    FOREIGN KEY (label_id) REFERENCES labels(id)
);
