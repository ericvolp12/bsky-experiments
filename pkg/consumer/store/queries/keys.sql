-- name: CreateKey :exec
INSERT INTO api_keys(api_key, auth_entity, assigned_user)
VALUES ($1, $2, $3);
-- name: DeleteKey :exec
DELETE FROM api_keys
WHERE api_key = $1;
-- name: GetKey :one
SELECT *
FROM api_keys
WHERE api_key = $1;
