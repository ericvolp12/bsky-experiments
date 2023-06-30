CREATE TABLE post_likes (
    post_id TEXT NOT NULL,
    author_did TEXT,
    like_count BIGINT NOT NULL,
    PRIMARY KEY (post_id)
);
