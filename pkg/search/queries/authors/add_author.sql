-- name: AddAuthor :exec
INSERT INTO authors (did, handle)
VALUES ($1, $2) ON CONFLICT (did) DO NOTHING;
