-- name: GetClusters :many
SELECT id, lookup_alias, name
FROM clusters;
