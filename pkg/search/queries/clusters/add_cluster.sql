-- name: AddCluster :one
INSERT INTO clusters (lookup_alias, name)
VALUES (sqlc.arg('lookup_alias'), sqlc.arg('name'))
RETURNING id, lookup_alias, name;
