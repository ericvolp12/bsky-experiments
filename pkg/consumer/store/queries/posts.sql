-- name: CreatePost :exec
INSERT INTO posts (
        actor_did,
        rkey,
        content,
        parent_post_actor_did,
        parent_post_rkey,
        parent_relationship,
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
        $10
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
