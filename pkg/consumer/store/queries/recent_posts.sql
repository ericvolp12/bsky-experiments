-- name: CreateRecentPost :exec
INSERT INTO recent_posts (
        actor_did,
        rkey,
        content,
        parent_post_actor_did,
        parent_post_rkey,
        quote_post_actor_did,
        quote_post_rkey,
        root_post_actor_did,
        root_post_rkey,
        has_embedded_media,
        facets,
        embed,
        langs,
        tags,
        subject_id,
        created_at
    )
VALUES (
        $1,
        $2,
        $3,
        $4,
        $5,
        $6,
        $7,
        $8,
        $9,
        $10,
        $11,
        $12,
        $13,
        $14,
        $15,
        $16
    );
-- name: DeleteRecentPost :exec
DELETE FROM recent_posts
WHERE actor_did = $1
    AND rkey = $2;
-- name: GetRecentPost :one
SELECT *
FROM recent_posts
WHERE actor_did = $1
    AND rkey = $2;
-- name: ListRecentPosts :many
SELECT *
FROM recent_posts
WHERE inserted_at < $1
ORDER BY inserted_at DESC
LIMIT $2;
-- name: GetRecentPostsByActor :many
SELECT *
FROM recent_posts
WHERE actor_did = $1
ORDER BY created_at DESC
LIMIT $2;
-- name: GetRecentPostsByActorsFollowingTarget :many
WITH followers AS (
    SELECT actor_did
    FROM follows
    WHERE target_did = $1
)
SELECT p.*
FROM recent_posts p
    JOIN followers f ON f.actor_did = p.actor_did
WHERE (p.created_at, p.actor_did, p.rkey) < (
        sqlc.arg('cursor_created_at')::TIMESTAMPTZ,
        sqlc.arg('cursor_actor_did')::TEXT,
        sqlc.arg('cursor_rkey')::TEXT
    )
    AND (p.root_post_rkey IS NULL)
    AND (
        (p.parent_relationship IS NULL)
        OR (p.parent_relationship <> 'r'::text)
    )
ORDER BY p.created_at DESC,
    p.actor_did DESC,
    p.rkey DESC
LIMIT $2;
-- name: GetRecentPostsFromNonSpamUsers :many
WITH non_spam AS (
    SELECT nm.actor_did
    FROM unnest(sqlc.arg('dids')::TEXT []) nm(actor_did)
        LEFT JOIN following_counts fc ON nm.actor_did = fc.actor_did
    WHERE fc.num_following < 4000
)
SELECT p.*
FROM recent_posts p
    JOIN non_spam f ON f.actor_did = p.actor_did
WHERE (p.created_at, p.actor_did, p.rkey) < (
        sqlc.arg('cursor_created_at')::TIMESTAMPTZ,
        sqlc.arg('cursor_actor_did')::TEXT,
        sqlc.arg('cursor_rkey')::TEXT
    )
    AND p.root_post_rkey IS NULL
    AND p.parent_post_rkey IS NULL
    AND p.created_at > NOW() - make_interval(hours := 24)
ORDER BY p.created_at DESC,
    p.actor_did DESC,
    p.rkey DESC
LIMIT $1;
-- name: GetRecentPostsPageByInsertedAt :many
SELECT *
FROM recent_posts
WHERE inserted_at > $1
ORDER BY inserted_at ASC
LIMIT $2;
-- name: GetRecentPostsFromNonMoots :many
WITH my_follows AS (
    SELECT target_did
    FROM follows
    WHERE follows.actor_did = $1
),
non_moots AS (
    SELECT actor_did
    FROM follows f
        LEFT JOIN my_follows ON f.actor_did = my_follows.target_did
    WHERE f.target_did = $1
        AND my_follows.target_did IS NULL
),
non_moots_and_non_spam AS (
    SELECT nm.actor_did
    FROM non_moots nm
        LEFT JOIN following_counts fc ON nm.actor_did = fc.actor_did
    WHERE fc.num_following < 4000
)
SELECT p.*
FROM recent_posts p
    JOIN non_moots_and_non_spam f ON f.actor_did = p.actor_did
WHERE (p.created_at, p.actor_did, p.rkey) < (
        sqlc.arg('cursor_created_at')::TIMESTAMPTZ,
        sqlc.arg('cursor_actor_did')::TEXT,
        sqlc.arg('cursor_rkey')::TEXT
    )
    AND p.root_post_rkey IS NULL
    AND p.parent_post_rkey IS NULL
    AND p.created_at > NOW() - make_interval(hours := 24)
ORDER BY p.created_at DESC,
    p.actor_did DESC,
    p.rkey DESC
LIMIT $2;
-- name: TrimOldRecentPosts :execrows
DELETE FROM recent_posts
WHERE created_at < NOW() - make_interval(hours := $1)
    OR created_at > NOW() + make_interval(mins := 15);
-- name: GetPopularRecentPostsByLanguage :many
select *
from recent_posts p
    JOIN follower_counts fc ON p.actor_did = fc.actor_did
WHERE fc.num_followers > sqlc.arg('min_followers')
    AND p.root_post_rkey IS NULL
    AND p.parent_post_rkey IS NULL
    AND sqlc.arg('lang')::TEXT = ANY (p.langs)
    AND (p.created_at, p.actor_did, p.rkey) < (
        sqlc.arg('cursor_created_at')::TIMESTAMPTZ,
        sqlc.arg('cursor_actor_did')::TEXT,
        sqlc.arg('cursor_rkey')::TEXT
    )
ORDER BY p.created_at DESC,
    p.actor_did DESC,
    p.rkey DESC
LIMIT sqlc.arg('limit');
-- name: GetTopPostsInWindow :many
SELECT p.*,
    lc.num_likes
from recent_posts p
    JOIN like_counts lc on p.subject_id = lc.subject_id
WHERE p.created_at > NOW() - MAKE_INTERVAL(hours := sqlc.arg('hours'))
    AND lc.num_likes > 10
ORDER BY lc.num_likes DESC
LIMIT sqlc.arg('limit');
