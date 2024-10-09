-- name: CreateImage :exec
INSERT INTO images (
        cid,
        post_actor_did,
        post_rkey,
        alt_text,
        is_video,
        created_at
    )
VALUES ($1, $2, $3, $4, $5, $6);
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
-- name: EnqueueImage :exec
INSERT INTO images_to_process (
        cid,
        post_actor_did,
        post_rkey,
        subject_id,
        alt_text,
        is_video
    )
VALUES ($1, $2, $3, $4, $5, $6);
-- name: ListImagesToProcess :many
SELECT *
FROM images_to_process
ORDER BY id ASC
LIMIT $1;
-- name: DequeueImages :exec
DELETE FROM images_to_process
WHERE id = ANY($1::BIGINT []);
