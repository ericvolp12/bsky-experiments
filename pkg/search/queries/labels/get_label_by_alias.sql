-- name: GetLabelByAlias :one
SELECT id, lookup_alias, name
FROM labels
WHERE lookup_alias = $1;
