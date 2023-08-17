-- name: UpsertActor :exec
INSERT INTO actors (
        did,
        handle,
        created_at,
        updated_at
    )
VALUES ($1, $2, $3, $3) ON CONFLICT (did) DO
UPDATE
SET handle = EXCLUDED.handle,
    updated_at = EXCLUDED.updated_at
WHERE actors.did = EXCLUDED.did;
-- name: GetActorByDID :one
SELECT *
FROM actors
WHERE did = $1;
-- name: GetActorByHandle :one
SELECT *
FROM actors
WHERE handle = $1;
-- name: FindActorsByHandle :many
SELECT *
FROM actors
WHERE handle ILIKE concat('%', $1, '%');
