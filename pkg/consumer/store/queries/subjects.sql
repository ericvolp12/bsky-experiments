-- name: CreateSubject :one
INSERT INTO subjects (actor_did, rkey, col)
VALUES ($1, $2, $3) ON CONFLICT (actor_did, rkey, col) DO
UPDATE
SET actor_did = subjects.actor_did -- a dummy update
RETURNING *;
