-- name: CreateBlock :exec
INSERT INTO blocks(
        actor_did,
        rkey,
        target_did,
        created_at
    )
VALUES ($1, $2, $3, $4);
-- name: DeleteBlock :exec
DELETE FROM blocks
WHERE actor_did = $1
    AND rkey = $2;
-- name: GetBlock :one
SELECT *
FROM blocks
WHERE actor_did = $1
    AND rkey = $2;
-- name: GetBlocksByActor :many
SELECT *
FROM blocks
WHERE actor_did = $1
ORDER BY created_at DESC
LIMIT $2;
-- name: GetBlocksByTarget :many
SELECT *
FROM blocks
WHERE target_did = $1
ORDER BY created_at DESC
LIMIT $2;
-- name: GetBlocksByActorAndTarget :many
SELECT *
FROM blocks
WHERE actor_did = $1
    AND target_did = $2
ORDER BY created_at DESC
LIMIT $3;
-- name: CountBlocksByActor :one
SELECT COUNT(*)
FROM blocks
WHERE actor_did = $1;
-- name: CountBlockersByTarget :one
SELECT COUNT(*)
FROM blocks
WHERE target_did = $1;
-- DeleteBlocksByActor :exec
DELETE FROM blocks
WHERE actor_did = $1;
-- DeleteBlocksByTarget :exec
DELETE FROM blocks
WHERE target_did = $1;
