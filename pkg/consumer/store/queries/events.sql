-- name: CreateEvent :one
INSERT INTO events (
        initiator_did,
        target_did,
        event_type,
        expired_at
    )
VALUES ($1, $2, $3, $4)
RETURNING id;
-- name: AddEventPost :exec
UPDATE events
SET post_uri = $2
WHERE id = $1;
-- name: GetEvent :one
SELECT *
FROM events
WHERE id = $1;
-- name: GetActiveEventsForInitiator :many
SELECT *
FROM events
WHERE initiator_did = $1
    AND event_type = $2
    AND (
        window_end > NOW()
        OR expired_at > NOW()
    )
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
    AND (
        window_end > NOW()
        OR expired_at < NOW()
    )
ORDER BY created_at DESC
LIMIT $3 OFFSET $4;
-- name: GetEventsForTarget :many
SELECT *
FROM events
WHERE $1 = target_did
ORDER BY created_at DESC
LIMIT $2 OFFSET $3;
-- name: GetUnconfirmedEvent :one
SELECT *
FROM events
WHERE post_uri = $1
    AND target_did = $2
    AND window_start IS NULL
    AND window_end IS NULL
    AND results IS NULL
LIMIT 1;
-- name: ConfirmEvent :exec
UPDATE events
SET window_start = $2,
    window_end = $3,
    expired_at = NULL
WHERE id = $1;
-- name: GetEventsToConclude :many
SELECT *
FROM events
WHERE window_end < NOW()
    AND results IS NULL
    AND event_type = $1
    AND expired_at IS NULL
ORDER BY window_end ASC
LIMIT $2 OFFSET $3;
-- name: ConcludeEvent :exec
UPDATE events
SET results = $2,
    concluded_at = $3
WHERE id = $1;
-- name: DeleteEvent :exec
DELETE FROM events
WHERE id = $1;
