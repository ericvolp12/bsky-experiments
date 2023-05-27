-- name: GetAuthorsByHandle :many
SELECT did, handle
FROM authors
WHERE handle = $1;
