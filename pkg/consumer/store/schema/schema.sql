-- Actors
CREATE TABLE actors (
    did TEXT NOT NULL,
    handle TEXT NOT NULL,
    pro_pic_cid TEXT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    inserted_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    PRIMARY KEY (did)
);
CREATE INDEX actors_created_at ON actors (created_at DESC);
CREATE INDEX actors_handle ON actors (handle);
CREATE INDEX actors_handle_trgm ON actors USING gin(handle gin_trgm_ops);
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
    has_embedded_media BOOLEAN DEFAULT FALSE NOT NULL,
    created_at TIMESTAMPTZ,
    inserted_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    PRIMARY KEY (actor_did, rkey)
);
CREATE INDEX posts_inserted_at ON posts (inserted_at DESC);
CREATE INDEX posts_created_at_index ON posts (created_at DESC);
CREATE INDEX posts_roots_or_quotes_only_created_at ON posts (created_at DESC)
WHERE root_post_rkey IS NULL
    AND parent_post_rkey IS NULL;
CREATE INDEX posts_parents ON posts (parent_post_actor_did, parent_post_rkey)
WHERE parent_post_actor_did IS NOT NULL
    AND parent_post_rkey IS NOT NULL;
-- Subjects
CREATE TABLE collections (
    id SERIAL PRIMARY KEY,
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
create index likes_actor_did_index on likes (actor_did);
-- Like Counts
CREATE TABLE like_counts (
    subject_id BIGINT NOT NULL,
    num_likes BIGINT NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    PRIMARY KEY (subject_id)
);
CREATE INDEX idx_like_counts_num_likes_gt_10 ON like_counts (subject_id)
WHERE num_likes > 10;
CREATE INDEX idx_like_counts_num_likes_gt_100 ON like_counts (subject_id)
WHERE num_likes > 100;
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
CREATE TABLE following_counts (
    actor_did TEXT NOT NULL,
    num_following BIGINT NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    PRIMARY KEY (actor_did)
);
CREATE INDEX following_counts_num_following_lt_4000 ON following_counts (actor_did)
WHERE num_following < 4000;
-- Images
CREATE TABLE images (
    cid TEXT NOT NULL,
    post_actor_did TEXT NOT NULL,
    post_rkey TEXT NOT NULL,
    alt_text TEXT,
    created_at TIMESTAMPTZ,
    inserted_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    PRIMARY KEY (post_actor_did, post_rkey, cid)
);
create index images_created_at_index on images (created_at desc);
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
CREATE MATERIALIZED VIEW recent_posts_with_score AS WITH RecentPosts AS (
    -- First CTE to narrow down the posts based on the given criteria
    SELECT p.actor_did,
        p.rkey,
        p.created_at
    FROM posts p
    WHERE p.created_at > (NOW() - INTERVAL '24 hours')
        AND p.created_at < (NOW() - INTERVAL '5 minutes')
        AND p.parent_post_rkey IS NULL
        AND p.root_post_rkey IS NULL
),
FilteredSubjects AS (
    -- Second CTE to determine the subject_ids for the filtered posts
    SELECT s.id AS subject_id,
        rp.actor_did,
        rp.rkey,
        rp.created_at
    FROM RecentPosts rp
        JOIN subjects s ON rp.actor_did = s.actor_did
        AND rp.rkey = s.rkey
    WHERE s.col = 1
),
FilteredLCs AS (
    SELECT lc.subject_id,
        fs.actor_did,
        fs.rkey,
        fs.created_at,
        lc.num_likes
    FROM like_counts lc
        JOIN FilteredSubjects fs ON fs.subject_id = lc.subject_id
    WHERE num_likes > 10
) -- Main query to compute the scores for the reduced set of posts
SELECT lc.actor_did,
    lc.subject_id,
    lc.rkey,
    lc.created_at,
    NOW() AS inserted_at,
    -- since it was present in the original materialized view
    (
        (COALESCE(lc.num_likes, 0) - 1) / (
            EXTRACT(
                EPOCH
                FROM now() - lc.created_at
            ) / 3600 + 2
        ) ^ 1.8
    )::float AS score
FROM FilteredLCs lc
ORDER BY score DESC;
CREATE INDEX recent_posts_with_score_score ON recent_posts_with_score (score DESC);
CREATE UNIQUE INDEX recent_posts_with_score_subject_id_idx ON recent_posts_with_score (subject_id);
-- Daily Stats View
CREATE MATERIALIZED VIEW daily_summary AS
SELECT COALESCE(
        likes_per_day.date,
        daily_active_likers.date,
        daily_active_posters.date,
        posts_per_day.date,
        posts_with_images_per_day.date,
        images_per_day.date,
        images_with_alt_text_per_day.date,
        first_time_posters.date,
        follows_per_day.date,
        daily_active_followers.date,
        blocks_per_day.date,
        daily_active_blockers.date
    )::TIMESTAMPTZ AS date,
    COALESCE(likes_per_day."Likes per Day", 0) AS "Likes per Day",
    COALESCE(daily_active_likers."Daily Active Likers", 0) AS "Daily Active Likers",
    COALESCE(daily_active_posters."Daily Active Posters", 0) AS "Daily Active Posters",
    COALESCE(posts_per_day."Posts per Day", 0) AS "Posts per Day",
    COALESCE(
        posts_with_images_per_day."Posts with Images per Day",
        0
    ) AS "Posts with Images per Day",
    COALESCE(images_per_day."Images per Day", 0) AS "Images per Day",
    COALESCE(
        images_with_alt_text_per_day."Images with Alt Text per Day",
        0
    ) AS "Images with Alt Text per Day",
    COALESCE(first_time_posters."First Time Posters", 0) AS "First Time Posters",
    COALESCE(follows_per_day."Follows per Day", 0) AS "Follows per Day",
    COALESCE(
        daily_active_followers."Daily Active Followers",
        0
    ) AS "Daily Active Followers",
    COALESCE(blocks_per_day."Blocks per Day", 0) AS "Blocks per Day",
    COALESCE(daily_active_blockers."Daily Active Blockers", 0) AS "Daily Active Blockers"
FROM (
        SELECT date_trunc('day', created_at) AS date,
            COUNT(*) AS "Likes per Day"
        FROM likes
        GROUP BY date
    ) AS likes_per_day
    FULL OUTER JOIN (
        SELECT date_trunc('day', created_at) AS date,
            COUNT(DISTINCT likes.actor_did) AS "Daily Active Likers"
        FROM likes
        GROUP BY date
    ) AS daily_active_likers ON likes_per_day.date = daily_active_likers.date
    FULL OUTER JOIN (
        SELECT date_trunc('day', created_at) AS date,
            COUNT(DISTINCT posts.actor_did) AS "Daily Active Posters"
        FROM posts
        GROUP BY date
    ) AS daily_active_posters ON COALESCE(likes_per_day.date, daily_active_likers.date) = daily_active_posters.date
    FULL OUTER JOIN (
        SELECT date_trunc('day', created_at) AS date,
            COUNT(*) AS "Posts per Day"
        FROM posts
        GROUP BY date
    ) AS posts_per_day ON COALESCE(
        likes_per_day.date,
        daily_active_likers.date,
        daily_active_posters.date
    ) = posts_per_day.date
    FULL OUTER JOIN (
        SELECT date_trunc('day', i.created_at) AS date,
            COUNT(DISTINCT i.post_actor_did || i.post_rkey) AS "Posts with Images per Day"
        FROM images i
        GROUP BY date
    ) AS posts_with_images_per_day ON COALESCE(
        likes_per_day.date,
        daily_active_likers.date,
        daily_active_posters.date,
        posts_per_day.date
    ) = posts_with_images_per_day.date
    FULL OUTER JOIN (
        SELECT date_trunc('day', created_at) AS date,
            COUNT(*) AS "Images per Day"
        FROM images
        GROUP BY date
    ) AS images_per_day ON COALESCE(
        likes_per_day.date,
        daily_active_likers.date,
        daily_active_posters.date,
        posts_per_day.date,
        posts_with_images_per_day.date
    ) = images_per_day.date
    FULL OUTER JOIN (
        SELECT date_trunc('day', created_at) AS date,
            COUNT(*) AS "Images with Alt Text per Day"
        FROM images
        WHERE alt_text IS NOT NULL
        GROUP BY date
    ) AS images_with_alt_text_per_day ON COALESCE(
        likes_per_day.date,
        daily_active_likers.date,
        daily_active_posters.date,
        posts_per_day.date,
        posts_with_images_per_day.date,
        images_per_day.date
    ) = images_with_alt_text_per_day.date
    FULL OUTER JOIN (
        SELECT date_trunc('day', p.first_post_time) AS date,
            COUNT(*) AS "First Time Posters"
        FROM (
                SELECT actor_did,
                    MIN(created_at) AS first_post_time
                FROM posts
                GROUP BY actor_did
            ) p
        GROUP BY 1
    ) AS first_time_posters ON COALESCE(
        likes_per_day.date,
        daily_active_likers.date,
        daily_active_posters.date,
        posts_per_day.date,
        posts_with_images_per_day.date,
        images_per_day.date,
        images_with_alt_text_per_day.date
    ) = first_time_posters.date
    FULL OUTER JOIN (
        SELECT date_trunc('day', created_at) AS date,
            COUNT(*) AS "Follows per Day"
        FROM follows
        GROUP BY date
    ) AS follows_per_day ON COALESCE(
        likes_per_day.date,
        daily_active_likers.date,
        daily_active_posters.date,
        posts_per_day.date,
        posts_with_images_per_day.date,
        images_per_day.date,
        images_with_alt_text_per_day.date,
        first_time_posters.date
    ) = follows_per_day.date
    FULL OUTER JOIN (
        SELECT date_trunc('day', created_at) AS date,
            COUNT(DISTINCT actor_did) AS "Daily Active Followers"
        FROM follows
        GROUP BY date
    ) AS daily_active_followers ON COALESCE(
        likes_per_day.date,
        daily_active_likers.date,
        daily_active_posters.date,
        posts_per_day.date,
        posts_with_images_per_day.date,
        images_per_day.date,
        images_with_alt_text_per_day.date,
        first_time_posters.date,
        follows_per_day.date
    ) = daily_active_followers.date
    FULL OUTER JOIN (
        SELECT date_trunc('day', created_at) AS date,
            COUNT(*) AS "Blocks per Day"
        FROM blocks
        GROUP BY date
    ) AS blocks_per_day ON COALESCE(
        likes_per_day.date,
        daily_active_likers.date,
        daily_active_posters.date,
        posts_per_day.date,
        posts_with_images_per_day.date,
        images_per_day.date,
        images_with_alt_text_per_day.date,
        first_time_posters.date,
        follows_per_day.date,
        daily_active_followers.date
    ) = blocks_per_day.date
    FULL OUTER JOIN (
        SELECT date_trunc('day', created_at) AS date,
            COUNT(DISTINCT actor_did) AS "Daily Active Blockers"
        FROM blocks
        GROUP BY date
    ) AS daily_active_blockers ON COALESCE(
        likes_per_day.date,
        daily_active_likers.date,
        daily_active_posters.date,
        posts_per_day.date,
        posts_with_images_per_day.date,
        images_per_day.date,
        images_with_alt_text_per_day.date,
        first_time_posters.date,
        follows_per_day.date,
        daily_active_followers.date,
        blocks_per_day.date
    ) = daily_active_blockers.date
ORDER BY date;
