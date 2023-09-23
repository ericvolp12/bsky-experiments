-- name: CreateSubject :one
INSERT INTO subjects (actor_did, rkey, col)
VALUES ($1, $2, $3) ON CONFLICT (actor_did, rkey, col) DO
UPDATE
SET actor_did = subjects.actor_did -- a dummy update
RETURNING *;
-- name: GetSubject :one
SELECT * FROM subjects WHERE actor_did = $1 AND rkey = $2 AND col = $3;
-- name: DeleteSubject :exec
DELETE FROM subjects WHERE actor_did = $1 AND rkey = $2 AND col = $3;
-- name: GetSubjectById :one
SELECT * FROM subjects WHERE id = $1;
