-- CREATE TABLE point_assignments (
--     id BIGSERIAL PRIMARY KEY,
--     event_id BIGINT NOT NULL,
--     actor_did TEXT NOT NULL,
--     points INTEGER NOT NULL,
--     created_at TIMESTAMP NOT NULL DEFAULT NOW(),
--     updated_at TIMESTAMP NOT NULL DEFAULT NOW()
-- );
-- name: CreatePointAssignment :exec
INSERT INTO point_assignments (
        event_id,
        actor_did,
        points
    )
VALUES ($1, $2, $3) ON CONFLICT (event_id, actor_did) DO
UPDATE
SET points = EXCLUDED.points;
-- name: GetPointAssignment :one
SELECT *
FROM point_assignments
WHERE event_id = $1
    AND actor_did = $2
LIMIT 1;
-- name: GetPointAssignmentsForEvent :many
SELECT *
FROM point_assignments
WHERE event_id = $1
ORDER BY created_at DESC
LIMIT $2 OFFSET $3;
-- name: GetPointAssignmentsForActor :many
SELECT *
FROM point_assignments
WHERE actor_did = $1
ORDER BY created_at DESC
LIMIT $2 OFFSET $3;
-- name: GetTotalPointsForActor :one
SELECT SUM(points)
FROM point_assignments
WHERE actor_did = $1;
-- name: GetTotalPointsForEvent :one
SELECT SUM(points)
FROM point_assignments
WHERE event_id = $1;
-- name: DeletePointAssignment :exec
DELETE FROM point_assignments
WHERE event_id = $1
    AND actor_did = $2;
-- name: UpatePointAssignment :exec
UPDATE point_assignments
SET points = $3
WHERE event_id = $1
    AND actor_did = $2;
