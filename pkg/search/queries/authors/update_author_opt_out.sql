-- name: UpdateAuthorOptOut :exec
UPDATE authors
SET cluster_opt_out = $2
WHERE did = $1;
