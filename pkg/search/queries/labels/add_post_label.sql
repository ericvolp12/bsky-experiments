-- name: AddPostLabel :exec
INSERT INTO post_labels (post_id, label)
VALUES ($1, $2)
ON CONFLICT (post_id, label) DO NOTHING;
