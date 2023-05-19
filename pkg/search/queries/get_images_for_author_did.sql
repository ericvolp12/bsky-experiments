-- name: GetImagesForAuthorDID :many
SELECT cid, post_id, author_did, alt_text, mime_type, fullsize_url, thumbnail_url, created_at, cv_completed, cv_run_at, cv_classes
FROM images
WHERE author_did = $1;
