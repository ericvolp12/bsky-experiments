CREATE TABLE post_labels (
    post_id TEXT NOT NULL,
    label TEXT NOT NULL,
    PRIMARY KEY (post_id, label),
    FOREIGN KEY (post_id) REFERENCES posts(id)
);
