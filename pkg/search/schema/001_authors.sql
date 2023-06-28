CREATE TABLE authors (
    did TEXT PRIMARY KEY,
    handle TEXT NOT NULL,
    cluster_opt_out BOOLEAN NOT NULL DEFAULT FALSE
);
