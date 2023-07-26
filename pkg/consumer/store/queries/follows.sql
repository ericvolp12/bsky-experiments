-- name: CreateFollow :exec
INSERT INTO follows(
        actor_did,
        rkey,
        target_did,
        created_at
    )
VALUES ($1, $2, $3, $4);
-- name: DeleteFollow :exec
DELETE FROM follows
WHERE actor_did = $1
    AND rkey = $2;
-- name: GetFollow :one
SELECT *
FROM follows
WHERE actor_did = $1
    AND rkey = $2;
-- name: GetFollowsByActor :many
SELECT *
FROM follows
WHERE actor_did = $1
ORDER BY created_at DESC
LIMIT $2;
-- name: GetFollowsByTarget :many
SELECT *
FROM follows
WHERE target_did = $1
ORDER BY created_at DESC
LIMIT $2;
-- name: GetFollowsByActorAndTarget :many
SELECT *
FROM follows
WHERE actor_did = $1
    AND target_did = $2
ORDER BY created_at DESC
LIMIT $3;
-- name: CountFollowsByActor :one
SELECT COUNT(*)
FROM follows
WHERE actor_did = $1;
-- name: CountFollowersByTarget :one
SELECT COUNT(*)
FROM follows
WHERE target_did = $1;
-- DeleteFollowsByActor :exec
DELETE FROM follows
WHERE actor_did = $1;
-- DeleteFollowsByTarget :exec
DELETE FROM follows
WHERE target_did = $1;
