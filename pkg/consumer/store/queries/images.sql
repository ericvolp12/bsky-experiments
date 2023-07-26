-- name: CreateImage :exec
INSERT INTO images (
        cid,
        post_actor_did,
        post_rkey,
        alt_text,
        created_at
    )
VALUES ($1, $2, $3, $4, $5);
-- name: DeleteImage :exec
DELETE FROM images
WHERE post_actor_did = $1
    AND post_rkey = $2
    AND cid = $3;
-- name: DeleteImagesForPost :exec
DELETE FROM images
WHERE post_actor_did = $1
    AND post_rkey = $2;
-- name: GetImage :one
SELECT *
FROM images
WHERE post_actor_did = $1
    AND post_rkey = $2
    AND cid = $3;
-- name: GetImagesForPost :many
SELECT *
FROM images
WHERE post_actor_did = $1
    AND post_rkey = $2
ORDER BY created_at DESC
LIMIT $3;
