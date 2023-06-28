-- name: GetAuthor :one
SELECT did,
    handle,
    cluster_opt_out
FROM authors
WHERE did = $1;
