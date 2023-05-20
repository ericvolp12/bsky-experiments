CREATE INDEX idx_posts_author_did ON posts (author_did);

create index parent_post_id
    on posts (parent_post_id, id);

create index text_tgm_idx
    on posts using gist (text gist_trgm_ops);
