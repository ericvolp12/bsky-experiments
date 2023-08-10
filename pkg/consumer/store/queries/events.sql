-- name: CreateEvent :exec
INSERT INTO events (
        initiator_did,
        target_did,
        event_type,
        completed_at
    )
VALUES ($1, $2, $3, $4);
-- name: GetEvent :one
SELECT *
FROM events
WHERE id = $1;
-- name: GetActiveEventsForInitiator :many
SELECT *
FROM events
WHERE initiator_did = $1
    AND event_type = $2
    AND completed_at > NOW()
ORDER BY created_at DESC
LIMIT $3 OFFSET $4;
-- name: GetEventsForInitiator :many
SELECT *
FROM events
WHERE initiator_did = $1
ORDER BY created_at DESC
LIMIT $2 OFFSET $3;
-- name: GetActiveEventsForTarget :many
SELECT *
FROM events
WHERE $1 = target_did
    AND event_type = $2
    AND completed_at > NOW()
ORDER BY created_at DESC
LIMIT $3 OFFSET $4;
-- name: GetEventsForTarget :many
SELECT *
FROM events
WHERE $1 = target_did
ORDER BY created_at DESC
LIMIT $2 OFFSET $3;
-- name: GetEventsToConclude :many
SELECT *
FROM events
WHERE completed_at < NOW()
    AND results IS NULL
    AND event_type = $1
ORDER BY completed_at ASC
LIMIT $2 OFFSET $3;
-- name: ConcludeEvent :exec
UPDATE events
SET results = $2,
    concluded_at = $3
WHERE id = $1;
-- name: DeleteEvent :exec
DELETE FROM events
WHERE id = $1;
