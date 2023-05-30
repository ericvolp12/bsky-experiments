-- name: AddLabel :one
INSERT INTO labels (lookup_alias, name)
VALUES (sqlc.arg('lookup_alias'), sqlc.arg('name'))
RETURNING id, lookup_alias, name;
