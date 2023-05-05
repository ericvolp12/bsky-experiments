-- name: GetPost :one
SELECT id, text, parent_post_id, root_post_id, author_did, created_at, has_embedded_media, parent_relationship
FROM posts
WHERE id = $1;
