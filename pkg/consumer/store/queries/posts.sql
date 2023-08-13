-- name: CreatePost :exec
INSERT INTO posts (
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
        $11
    );
-- name: DeletePost :exec
DELETE FROM posts
WHERE actor_did = $1
    AND rkey = $2;
-- name: GetPost :one
SELECT *
FROM posts
WHERE actor_did = $1
    AND rkey = $2;
-- name: GetPostsByActor :many
SELECT *
FROM posts
WHERE actor_did = $1
ORDER BY created_at DESC
LIMIT $2;
-- name: GetPostsByActorsFollowingTarget :many
WITH followers AS (
    SELECT actor_did
    FROM follows
    WHERE target_did = $1
)
SELECT p.*
FROM posts p
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
-- name: GetPostsFromNonMoots :many
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
FROM posts p
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
-- name: GetMyPostsByFuzzyContent :many
SELECT *
FROM posts
WHERE actor_did = $1
    AND content ILIKE concat('%', sqlc.arg('query')::text, '%')::text
    AND content not ilike '%!jazbot%'
ORDER BY created_at DESC
LIMIT $2 OFFSET $3;
