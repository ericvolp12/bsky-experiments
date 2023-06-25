-- name: SetPostIndexedTimestamp :exec
UPDATE posts
SET indexed_at = $1
WHERE id = ANY(sqlc.arg('post_ids')::text []);
