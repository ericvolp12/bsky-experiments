-- name: GetAllLabels :many
SELECT labels.id, labels.name, labels.lookup_alias
FROM labels
ORDER BY labels.id ASC
LIMIT sqlc.arg('limit')
OFFSET sqlc.arg('offset');
