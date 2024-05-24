-- name: RefreshStatsForDay :exec
WITH stats AS (
    SELECT COALESCE(
            likes_per_day.date,
            daily_active_likers.date,
            daily_active_posters.date,
            posts_per_day.date,
            posts_with_images_per_day.date,
            images_per_day.date,
            images_with_alt_text_per_day.date,
            follows_per_day.date,
            daily_active_followers.date,
            blocks_per_day.date,
            daily_active_blockers.date
        ) AS date,
        COALESCE(likes_per_day.likes_per_day, 0) AS likes_per_day,
        COALESCE(daily_active_likers.daily_active_likers, 0) AS daily_active_likers,
        COALESCE(daily_active_posters.daily_active_posters, 0) AS daily_active_posters,
        COALESCE(posts_per_day.posts_per_day, 0) AS posts_per_day,
        COALESCE(
            posts_with_images_per_day.posts_with_images_per_day,
            0
        ) AS posts_with_images_per_day,
        COALESCE(images_per_day.images_per_day, 0) AS images_per_day,
        COALESCE(
            images_with_alt_text_per_day.images_with_alt_text_per_day,
            0
        ) AS images_with_alt_text_per_day,
        COALESCE(follows_per_day.follows_per_day, 0) AS follows_per_day,
        COALESCE(daily_active_followers.daily_active_followers, 0) AS daily_active_followers,
        COALESCE(blocks_per_day.blocks_per_day, 0) AS blocks_per_day,
        COALESCE(daily_active_blockers.daily_active_blockers, 0) AS daily_active_blockers
    FROM (
            SELECT date_trunc('day', created_at) AS date,
                COUNT(*) AS likes_per_day
            FROM likes
            WHERE created_at > date_trunc(
                    'day',
                    NOW() + make_interval(days := sqlc.arg('start_today_offset'))
                )
                AND created_at < date_trunc(
                    'day',
                    NOW() + make_interval(days := sqlc.arg('end_today_offset'))
                )
            GROUP BY date
        ) AS likes_per_day
        FULL OUTER JOIN (
            SELECT date_trunc('day', created_at) AS date,
                COUNT(DISTINCT likes.actor_did) AS daily_active_likers
            FROM likes
            WHERE created_at > date_trunc(
                    'day',
                    NOW() + make_interval(days := sqlc.arg('start_today_offset'))
                )
                AND created_at < date_trunc(
                    'day',
                    NOW() + make_interval(days := sqlc.arg('end_today_offset'))
                )
            GROUP BY date
        ) AS daily_active_likers ON likes_per_day.date = daily_active_likers.date
        FULL OUTER JOIN (
            SELECT date_trunc('day', created_at) AS date,
                COUNT(DISTINCT posts.actor_did) AS daily_active_posters
            FROM posts
            WHERE created_at > date_trunc(
                    'day',
                    NOW() + make_interval(days := sqlc.arg('start_today_offset'))
                )
                AND created_at < date_trunc(
                    'day',
                    NOW() + make_interval(days := sqlc.arg('end_today_offset'))
                )
            GROUP BY date
        ) AS daily_active_posters ON COALESCE(likes_per_day.date, daily_active_likers.date) = daily_active_posters.date
        FULL OUTER JOIN (
            SELECT date_trunc('day', created_at) AS date,
                COUNT(*) AS posts_per_day
            FROM posts
            WHERE created_at > date_trunc(
                    'day',
                    NOW() + make_interval(days := sqlc.arg('start_today_offset'))
                )
                AND created_at < date_trunc(
                    'day',
                    NOW() + make_interval(days := sqlc.arg('end_today_offset'))
                )
            GROUP BY date
        ) AS posts_per_day ON COALESCE(
            likes_per_day.date,
            daily_active_likers.date,
            daily_active_posters.date
        ) = posts_per_day.date
        FULL OUTER JOIN (
            SELECT date_trunc('day', i.created_at) AS date,
                COUNT(DISTINCT i.post_actor_did || i.post_rkey) AS posts_with_images_per_day
            FROM images i
            WHERE created_at > date_trunc(
                    'day',
                    NOW() + make_interval(days := sqlc.arg('start_today_offset'))
                )
                AND created_at < date_trunc(
                    'day',
                    NOW() + make_interval(days := sqlc.arg('end_today_offset'))
                )
            GROUP BY date
        ) AS posts_with_images_per_day ON COALESCE(
            likes_per_day.date,
            daily_active_likers.date,
            daily_active_posters.date,
            posts_per_day.date
        ) = posts_with_images_per_day.date
        FULL OUTER JOIN (
            SELECT date_trunc('day', created_at) AS date,
                COUNT(*) AS images_per_Day
            FROM images
            WHERE created_at > date_trunc(
                    'day',
                    NOW() + make_interval(days := sqlc.arg('start_today_offset'))
                )
                AND created_at < date_trunc(
                    'day',
                    NOW() + make_interval(days := sqlc.arg('end_today_offset'))
                )
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
                COUNT(*) AS images_with_alt_text_per_day
            FROM images
            WHERE created_at > date_trunc(
                    'day',
                    NOW() + make_interval(days := sqlc.arg('start_today_offset'))
                )
                AND created_at < date_trunc(
                    'day',
                    NOW() + make_interval(days := sqlc.arg('end_today_offset'))
                )
                AND alt_text IS NOT NULL
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
            SELECT date_trunc('day', created_at) AS date,
                COUNT(*) AS follows_per_day
            FROM follows
            WHERE created_at > date_trunc(
                    'day',
                    NOW() + make_interval(days := sqlc.arg('start_today_offset'))
                )
                AND created_at < date_trunc(
                    'day',
                    NOW() + make_interval(days := sqlc.arg('end_today_offset'))
                )
            GROUP BY date
        ) AS follows_per_day ON COALESCE(
            likes_per_day.date,
            daily_active_likers.date,
            daily_active_posters.date,
            posts_per_day.date,
            posts_with_images_per_day.date,
            images_per_day.date,
            images_with_alt_text_per_day.date
        ) = follows_per_day.date
        FULL OUTER JOIN (
            SELECT date_trunc('day', created_at) AS date,
                COUNT(DISTINCT actor_did) AS daily_active_followers
            FROM follows
            WHERE created_at > date_trunc(
                    'day',
                    NOW() + make_interval(days := sqlc.arg('start_today_offset'))
                )
                AND created_at < date_trunc(
                    'day',
                    NOW() + make_interval(days := sqlc.arg('end_today_offset'))
                )
            GROUP BY date
        ) AS daily_active_followers ON COALESCE(
            likes_per_day.date,
            daily_active_likers.date,
            daily_active_posters.date,
            posts_per_day.date,
            posts_with_images_per_day.date,
            images_per_day.date,
            images_with_alt_text_per_day.date,
            follows_per_day.date
        ) = daily_active_followers.date
        FULL OUTER JOIN (
            SELECT date_trunc('day', created_at) AS date,
                COUNT(*) AS blocks_per_day
            FROM blocks
            WHERE created_at > date_trunc(
                    'day',
                    NOW() + make_interval(days := sqlc.arg('start_today_offset'))
                )
                AND created_at < date_trunc(
                    'day',
                    NOW() + make_interval(days := sqlc.arg('end_today_offset'))
                )
            GROUP BY date
        ) AS blocks_per_day ON COALESCE(
            likes_per_day.date,
            daily_active_likers.date,
            daily_active_posters.date,
            posts_per_day.date,
            posts_with_images_per_day.date,
            images_per_day.date,
            images_with_alt_text_per_day.date,
            follows_per_day.date,
            daily_active_followers.date
        ) = blocks_per_day.date
        FULL OUTER JOIN (
            SELECT date_trunc('day', created_at) AS date,
                COUNT(DISTINCT actor_did) AS daily_active_blockers
            FROM blocks
            WHERE created_at > date_trunc(
                    'day',
                    NOW() + make_interval(days := sqlc.arg('start_today_offset'))
                )
                AND created_at < date_trunc(
                    'day',
                    NOW() + make_interval(days := sqlc.arg('end_today_offset'))
                )
            GROUP BY date
        ) AS daily_active_blockers ON COALESCE(
            likes_per_day.date,
            daily_active_likers.date,
            daily_active_posters.date,
            posts_per_day.date,
            posts_with_images_per_day.date,
            images_per_day.date,
            images_with_alt_text_per_day.date,
            follows_per_day.date,
            daily_active_followers.date,
            blocks_per_day.date
        ) = daily_active_blockers.date
)
INSERT INTO daily_summary (
        date,
        "Likes per Day",
        "Daily Active Likers",
        "Daily Active Posters",
        "Posts per Day",
        "Posts with Images per Day",
        "Images per Day",
        "Images with Alt Text per Day",
        "First Time Posters",
        "Follows per Day",
        "Daily Active Followers",
        "Blocks per Day",
        "Daily Active Blockers"
    )
SELECT date,
    likes_per_day,
    daily_active_likers,
    daily_active_posters,
    posts_per_day,
    posts_with_images_per_day,
    images_per_day,
    images_with_alt_text_per_day,
    0,
    follows_per_day,
    daily_active_followers,
    blocks_per_day,
    daily_active_blockers
FROM stats ON CONFLICT (date) DO
UPDATE
SET "Likes per Day" = EXCLUDED."Likes per Day",
    "Daily Active Likers" = EXCLUDED."Daily Active Likers",
    "Daily Active Posters" = EXCLUDED."Daily Active Posters",
    "Posts per Day" = EXCLUDED."Posts per Day",
    "Posts with Images per Day" = EXCLUDED."Posts with Images per Day",
    "Images per Day" = EXCLUDED."Images per Day",
    "Images with Alt Text per Day" = EXCLUDED."Images with Alt Text per Day",
    "First Time Posters" = EXCLUDED."First Time Posters",
    "Follows per Day" = EXCLUDED."Follows per Day",
    "Daily Active Followers" = EXCLUDED."Daily Active Followers",
    "Blocks per Day" = EXCLUDED."Blocks per Day",
    "Daily Active Blockers" = EXCLUDED."Daily Active Blockers";
