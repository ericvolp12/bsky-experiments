-- name: GetAuthor :one
SELECT did, handle
FROM authors
WHERE did = $1;
