-- name: UpdateImage :exec
UPDATE images
    SET post_id = $2, author_did = $3, alt_text = $4, mime_type = $5, fullsize_url = $6, thumbnail_url = $7, created_at = $8, cv_completed = $9, cv_run_at = $10, cv_classes = $11
    WHERE cid = $1;
