-- name: AddImage :exec
INSERT INTO images (
        cid,
        post_id,
        author_did,
        alt_text,
        mime_type,
        created_at,
        cv_completed,
        cv_run_at,
        cv_classes
    )
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);
