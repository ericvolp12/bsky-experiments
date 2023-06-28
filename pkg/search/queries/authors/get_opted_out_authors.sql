-- name: GetOptedOutAuthors :many
SELECT did,
    handle,
    cluster_opt_out
FROM authors
WHERE cluster_opt_out = TRUE;
