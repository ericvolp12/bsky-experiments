-- name: CreateCollection :one
INSERT INTO collections (name) VALUES ($1) RETURNING *;
-- name: GetCollection :one
SELECT * FROM collections WHERE name = $1;
-- name: DeleteCollection :exec
DELETE FROM collections WHERE name = $1;
