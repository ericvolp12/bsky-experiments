-- name: AddPostLabel :exec
INSERT INTO post_labels (post_id, author_did, label)
VALUES ($1, $2, $3) ON CONFLICT (post_id, label) DO NOTHING;
