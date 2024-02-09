-- name: GetUnprocessedImages :many
SELECT cid,
    post_id,
    author_did,
    alt_text,
    mime_type,
    created_at,
    cv_completed,
    cv_run_at,
    cv_classes
FROM images
WHERE cv_completed = FALSE
ORDER BY created_at ASC
LIMIT $1;
