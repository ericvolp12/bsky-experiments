-- name: UpdateImage :exec
UPDATE images
SET author_did = $3,
    alt_text = $4,
    mime_type = $5,
    created_at = $6,
    cv_completed = $7,
    cv_run_at = $8,
    cv_classes = $9
WHERE cid = $1
    and post_id = $2;
