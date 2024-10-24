-- Actors
CREATE TABLE actors (
    did TEXT NOT NULL,
    handle TEXT NOT NULL,
    display_name TEXT,
    bio TEXT,
    handle_valid BOOLEAN DEFAULT FALSE NOT NULL,
    last_validated TIMESTAMPTZ,
    pro_pic_cid TEXT,
    banner_cid TEXT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    inserted_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    id BIGSERIAL,
    PRIMARY KEY (did)
);
CREATE INDEX actors_created_at ON actors (created_at DESC);
CREATE INDEX actors_handle ON actors (handle);
CREATE INDEX actors_handle_trgm ON actors USING gin(handle gin_trgm_ops);
CREATE INDEX actors_id_idx ON actors (id);
CREATE INDEX actors_no_pro_pic ON actors (pro_pic_cid)
WHERE pro_pic_cid IS NULL;
-- Posts
CREATE TABLE posts (
    actor_did TEXT NOT NULL,
    rkey TEXT NOT NULL,
    content TEXT,
    parent_post_actor_did TEXT,
    quote_post_actor_did TEXT,
    quote_post_rkey TEXT,
    parent_post_rkey TEXT,
    root_post_actor_did TEXT,
    root_post_rkey TEXT,
    facets JSONB,
    embed JSONB,
    langs TEXT [],
    tags TEXT [],
    subject_id BIGINT,
    has_embedded_media BOOLEAN DEFAULT FALSE NOT NULL,
    created_at TIMESTAMPTZ,
    inserted_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    PRIMARY KEY (actor_did, rkey)
);
CREATE INDEX posts_inserted_at ON posts (inserted_at DESC);
CREATE INDEX posts_created_at_index ON posts (created_at DESC);
CREATE INDEX posts_subject_id ON posts (subject_id);
CREATE INDEX posts_roots_or_quotes_only_created_at ON posts (created_at DESC)
WHERE root_post_rkey IS NULL
    AND parent_post_rkey IS NULL;
CREATE INDEX posts_parents ON posts (parent_post_actor_did, parent_post_rkey)
WHERE parent_post_actor_did IS NOT NULL
    AND parent_post_rkey IS NOT NULL;
-- Posts
CREATE TABLE recent_posts (
    actor_did TEXT NOT NULL,
    rkey TEXT NOT NULL,
    content TEXT,
    parent_post_actor_did TEXT,
    quote_post_actor_did TEXT,
    quote_post_rkey TEXT,
    parent_post_rkey TEXT,
    root_post_actor_did TEXT,
    root_post_rkey TEXT,
    facets JSONB,
    embed JSONB,
    langs TEXT [],
    tags TEXT [],
    subject_id BIGINT,
    has_embedded_media BOOLEAN DEFAULT FALSE NOT NULL,
    created_at TIMESTAMPTZ,
    inserted_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    PRIMARY KEY (actor_did, rkey)
);
CREATE INDEX recent_posts_inserted_at ON recent_posts (inserted_at DESC);
CREATE INDEX recent_posts_created_at_index ON recent_posts (created_at DESC);
CREATE INDEX recent_posts_did_rkey_created_at ON recent_posts (actor_did DESC, rkey DESC, created_at DESC);
CREATE INDEX recent_posts_did_created_at_rkey ON recent_posts (actor_did, created_at DESC, rkey DESC);
CREATE INDEX recent_posts_by_subject ON recent_posts (subject_id);
CREATE INDEX recent_posts_roots_or_quotes_only_created_at ON recent_posts (created_at DESC)
WHERE root_post_rkey IS NULL
    AND parent_post_rkey IS NULL;
CREATE INDEX recent_posts_parents ON recent_posts (parent_post_actor_did, parent_post_rkey)
WHERE parent_post_actor_did IS NOT NULL
    AND parent_post_rkey IS NOT NULL;
-- Subjects
CREATE TABLE collections (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    UNIQUE (name)
);
CREATE TABLE subjects (
    id BIGSERIAL PRIMARY KEY,
    actor_did TEXT NOT NULL,
    rkey TEXT NOT NULL,
    col INTEGER NOT NULL,
    UNIQUE (actor_did, col, rkey)
);
-- Reposts
CREATE TABLE reposts (
    actor_did TEXT NOT NULL,
    rkey TEXT NOT NULL,
    subj BIGINT NOT NULL,
    created_at TIMESTAMPTZ,
    inserted_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    PRIMARY KEY (actor_did, rkey)
);
create index reposts_created_at on reposts (created_at desc);
CREATE INDEX reposts_subject ON reposts (subj);
-- Repost Counts
CREATE TABLE repost_counts (
    subject_id BIGINT NOT NULL,
    num_reposts BIGINT NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    PRIMARY KEY (subject_id)
);
CREATE INDEX idx_repost_counts_num_reposts_gt_10 ON repost_counts (subject_id)
WHERE num_reposts > 10;
-- Likes
CREATE TABLE likes (
    actor_did TEXT NOT NULL,
    rkey TEXT NOT NULL,
    subj BIGINT NOT NULL,
    created_at TIMESTAMPTZ,
    inserted_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    PRIMARY KEY (actor_did, rkey)
);
create index likes_created_at on likes (created_at desc);
CREATE INDEX likes_subject ON likes (subj);
-- Like Counts
CREATE TABLE like_counts (
    subject_id BIGINT NOT NULL,
    num_likes BIGINT NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    subject_created_at TIMESTAMPTZ,
    PRIMARY KEY (subject_id)
);
CREATE INDEX idx_like_counts_num_likes_gt_10 ON like_counts (subject_id)
WHERE num_likes > 10;
CREATE INDEX idx_like_counts_num_likes_gt_100 ON like_counts (subject_id)
WHERE num_likes > 100;
CREATE INDEX idx_like_counts_num_likes_gt_100_cat ON like_counts (subject_created_at DESC)
WHERE num_likes > 100
    AND subject_created_at IS NOT NULL;
-- Blocks
CREATE TABLE blocks (
    actor_did TEXT NOT NULL,
    rkey TEXT NOT NULL,
    target_did TEXT NOT NULL,
    created_at TIMESTAMPTZ,
    inserted_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    PRIMARY KEY (actor_did, rkey)
);
create index blocks_created_at_index on blocks (created_at desc);
-- Follows
CREATE TABLE follows (
    actor_did TEXT NOT NULL,
    rkey TEXT NOT NULL,
    target_did TEXT NOT NULL,
    created_at TIMESTAMPTZ,
    inserted_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    PRIMARY KEY (actor_did, rkey)
);
create index follows_created_at_index on follows (created_at desc);
CREATE INDEX follows_target ON follows (target_did);
CREATE INDEX follows_actor ON follows (actor_did);
-- Follow Counts
CREATE TABLE follower_counts (
    actor_did TEXT NOT NULL,
    num_followers BIGINT NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    PRIMARY KEY (actor_did)
);
CREATE INDEX follower_counts_gt_500 ON follower_counts (actor_did)
WHERE num_followers > 500;
CREATE TABLE following_counts (
    actor_did TEXT NOT NULL,
    num_following BIGINT NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    PRIMARY KEY (actor_did)
);
CREATE INDEX following_counts_on_actor_did_and_num_following ON following_counts(actor_did, num_following)
WHERE num_following < 4000;
-- Images
CREATE TABLE images (
    cid TEXT NOT NULL,
    post_actor_did TEXT NOT NULL,
    post_rkey TEXT NOT NULL,
    alt_text TEXT,
    is_video BOOLEAN DEFAULT FALSE NOT NULL,
    created_at TIMESTAMPTZ,
    inserted_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    PRIMARY KEY (post_actor_did, post_rkey, cid)
);
create index images_created_at_index on images (created_at desc);
CREATE TABLE images_to_process (
    id BIGSERIAL PRIMARY KEY,
    cid TEXT NOT NULL,
    post_actor_did TEXT NOT NULL,
    post_rkey TEXT NOT NULL,
    subject_id BIGINT NOT NULL,
    alt_text TEXT,
    is_video BOOLEAN DEFAULT FALSE NOT NULL
);
-- Backfill Status
CREATE TABLE repo_backfill_status (
    repo TEXT NOT NULL,
    last_backfill TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    seq_started BIGINT DEFAULT 0 NOT NULL,
    state TEXT DEFAULT 'in_progress'::text NOT NULL,
    PRIMARY KEY (repo)
);
-- Routines for Like Count
CREATE FUNCTION update_updated_at() RETURNS TRIGGER AS $$ BEGIN NEW.updated_at = now();
RETURN NEW;
END;
$$ language 'plpgsql';
CREATE TRIGGER update_like_count_updated_at BEFORE
UPDATE ON like_counts FOR EACH ROW EXECUTE PROCEDURE update_updated_at();
-- Hotness View
CREATE MATERIALIZED VIEW recent_posts_with_score AS WITH RecentHotSubjects AS (
    SELECT lc.subject_id,
        lc.subject_created_at,
        lc.num_likes
    FROM like_counts lc
    WHERE lc.subject_created_at > (NOW() - INTERVAL '24 hours')
        AND lc.subject_created_at < (NOW() - INTERVAL '5 minutes')
        AND lc.num_likes > 100
),
FilteredSubjects AS (
    SELECT s.id,
        s.actor_did,
        s.rkey,
        rhs.subject_created_at,
        rhs.num_likes
    FROM RecentHotSubjects rhs
        JOIN subjects s ON rhs.subject_id = s.id
    WHERE s.col = 1
),
FilteredPosts AS (
    SELECT fs.id,
        fs.actor_did,
        fs.rkey,
        fs.subject_created_at,
        fs.num_likes,
        p.inserted_at,
        p.langs,
        p.has_embedded_media
    FROM FilteredSubjects fs
        JOIN posts p ON fs.actor_did = p.actor_did
        AND fs.rkey = p.rkey
    WHERE p.parent_post_rkey IS NULL
        AND p.root_post_rkey IS NULL
)
SELECT fp.id as subject_id,
    fp.actor_did,
    fp.rkey,
    fp.subject_created_at,
    fp.inserted_at,
    fp.langs,
    fp.has_embedded_media,
    (
        (COALESCE(fp.num_likes, 0) - 1) / (
            EXTRACT(
                EPOCH
                FROM now() - fp.subject_created_at
            ) / 3600 + 2
        ) ^ 1.8
    )::float AS score
FROM FilteredPosts fp
ORDER BY score DESC;
CREATE INDEX recent_posts_with_score_score ON recent_posts_with_score (score DESC);
CREATE UNIQUE INDEX recent_posts_with_score_subject_id_idx ON recent_posts_with_score (subject_id);
-- Follower Stats View
CREATE MATERIALIZED VIEW follower_stats AS WITH all_counts AS (
    SELECT a.did,
        COALESCE(fc.num_followers, 0) AS followers
    FROM actors a
        LEFT JOIN follower_counts fc ON a.did = fc.actor_did
),
percentiles AS (
    SELECT percentile_cont(
            ARRAY [0.25, 0.50, 0.75, 0.90, 0.95, 0.99, 0.995, 0.997, 0.999, 0.9999]
        ) WITHIN GROUP (
            ORDER BY followers
        ) AS pct
    FROM all_counts
)
SELECT pct [1]::float AS p25,
    pct [2]::float AS p50,
    pct [3]::float AS p75,
    pct [4]::float AS p90,
    pct [5]::float AS p95,
    pct [6]::float AS p99,
    pct [7]::float AS p99_5,
    pct [8]::float AS p99_7,
    pct [9]::float AS p99_9,
    pct [10]::float AS p99_99
FROM percentiles;
-- Daily Stats View
CREATE TABLE daily_summary (
    date date PRIMARY KEY,
    "Likes per Day" BIGINT NOT NULL,
    "Daily Active Likers" BIGINT NOT NULL,
    "Daily Active Posters" BIGINT NOT NULL,
    "Posts per Day" BIGINT NOT NULL,
    "Posts with Images per Day" BIGINT NOT NULL,
    "Images per Day" BIGINT NOT NULL,
    "Images with Alt Text per Day" BIGINT NOT NULL,
    "First Time Posters" BIGINT NOT NULL,
    "Follows per Day" BIGINT NOT NULL,
    "Daily Active Followers" BIGINT NOT NULL,
    "Blocks per Day" BIGINT NOT NULL,
    "Daily Active Blockers" BIGINT NOT NULL
);
CREATE INDEX daily_summary_date ON daily_summary (date);
-- Monthly Stats View
CREATE TABLE monthly_summary (
    date date PRIMARY KEY,
    "Likes per Month" BIGINT NOT NULL,
    "Monthly Active Likers" BIGINT NOT NULL,
    "Monthly Active Posters" BIGINT NOT NULL,
    "Posts per Month" BIGINT NOT NULL,
    "Posts with Images per Month" BIGINT NOT NULL,
    "Images per Month" BIGINT NOT NULL,
    "Images with Alt Text per Month" BIGINT NOT NULL,
    "First Time Posters" BIGINT NOT NULL,
    "Follows per Month" BIGINT NOT NULL,
    "Monthly Active Followers" BIGINT NOT NULL,
    "Blocks per Month" BIGINT NOT NULL,
    "Monthly Active Blockers" BIGINT NOT NULL
);
CREATE INDEX monthly_summary_date ON monthly_summary (date);
-- Stats Roaring Bitmaps
CREATE TABLE stats_bitmaps (
    id TEXT PRIMARY KEY,
    bitmap BYTEA,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL
);
CREATE INDEX stats_bitmaps_updated_at ON stats_bitmaps (updated_at DESC);