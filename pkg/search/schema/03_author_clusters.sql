CREATE TABLE author_clusters (
    author_did TEXT NOT NULL,
    cluster_id INT NOT NULL,
    PRIMARY KEY (author_did),
    FOREIGN KEY (author_did) REFERENCES authors(did),
    FOREIGN KEY (cluster_id) REFERENCES clusters(id)
);
