-- name: AddPost :exec
INSERT INTO posts (id, text, parent_post_id, root_post_id, author_did, created_at, has_embedded_media, parent_relationship)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8);
