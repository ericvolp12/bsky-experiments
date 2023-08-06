-- Posts
CREATE TABLE posts (
    actor_did TEXT NOT NULL,
    rkey TEXT NOT NULL,
    content TEXT,
    parent_post_actor_did TEXT,
    parent_post_rkey TEXT,
    parent_relationship TEXT,
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
WHERE (root_post_rkey IS NULL)
    AND (
        (parent_relationship IS NULL)
        OR (parent_relationship <> 'r'::text)
    );
-- Likes
CREATE TABLE likes (
    actor_did TEXT NOT NULL,
    rkey TEXT NOT NULL,
    subject_actor_did TEXT NOT NULL,
    subject_namespace TEXT NOT NULL,
    subject_rkey TEXT NOT NULL,
    created_at TIMESTAMPTZ,
    inserted_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    PRIMARY KEY (actor_did, rkey)
);
CREATE INDEX likes_inserted_at ON likes (inserted_at DESC);
CREATE INDEX likes_subject ON likes (
    subject_actor_did,
    subject_namespace,
    subject_rkey
);
CREATE TABLE like_counts (
    actor_did TEXT NOT NULL,
    ns TEXT NOT NULL,
    rkey TEXT NOT NULL,
    num_likes BIGINT NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    PRIMARY KEY (actor_did, ns, rkey)
);
CREATE INDEX idx_like_counts_num_likes_gt_10 ON like_counts (actor_did, rkey)
WHERE num_likes > 10;
-- Blocks
CREATE TABLE blocks (
    actor_did TEXT NOT NULL,
    rkey TEXT NOT NULL,
    target_did TEXT NOT NULL,
    created_at TIMESTAMPTZ,
    inserted_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    PRIMARY KEY (actor_did, rkey)
);
-- Follows
CREATE TABLE follows (
    actor_did TEXT NOT NULL,
    rkey TEXT NOT NULL,
    target_did TEXT NOT NULL,
    created_at TIMESTAMPTZ,
    inserted_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    PRIMARY KEY (actor_did, rkey)
);
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
CREATE MATERIALIZED VIEW recent_posts_with_score AS
SELECT p.actor_did,
    p.rkey,
    p.created_at,
    p.inserted_at,
    (
        (COALESCE(lc.num_likes, 0) - 1) / (
            EXTRACT(
                EPOCH
                FROM now() - p.created_at
            ) / 3600 + 2
        ) ^ 1.8
    )::float AS score
FROM posts p
    LEFT JOIN like_counts lc ON p.actor_did = lc.actor_did
    AND p.rkey = lc.rkey
WHERE p.created_at > (NOW() - INTERVAL '24 hours')
    AND p.created_at < (NOW() - INTERVAL '5 minutes')
    AND (
        p.parent_relationship is NULL
        OR p.parent_relationship != 'r'
    )
    AND p.root_post_rkey is NULL
    AND lc.num_likes > 10
ORDER BY score DESC;
CREATE INDEX recent_posts_with_score_score ON recent_posts_with_score (score DESC);
CREATE UNIQUE INDEX recent_posts_with_score_actor_rkey ON recent_posts_with_score (actor_did, rkey);
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
    ) AS date,
    likes_per_day."Likes per Day",
    daily_active_likers."Daily Active Likers",
    daily_active_posters."Daily Active Posters",
    posts_per_day."Posts per Day",
    posts_with_images_per_day."Posts with Images per Day",
    images_per_day."Images per Day",
    images_with_alt_text_per_day."Images with Alt Text per Day",
    first_time_posters."First Time Posters",
    follows_per_day."Follows per Day",
    daily_active_followers."Daily Active Followers",
    blocks_per_day."Blocks per Day",
    daily_active_blockers."Daily Active Blockers"
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
