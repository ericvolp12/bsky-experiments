-- name: GetLabels :many
SELECT id, lookup_alias, name
FROM labels
LIMIT $1
OFFSET $2;
