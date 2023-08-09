-- name: CreateLike :exec
WITH collection_ins AS (
    INSERT INTO collections (name)
    VALUES (sqlc.arg('collection')) ON CONFLICT (name) DO NOTHING
    RETURNING id
),
subject_ins AS (
    INSERT INTO subjects (actor_did, rkey, col)
    VALUES (
            sqlc.arg('subject_actor_did'),
            sqlc.arg('subject_rkey'),
            COALESCE(
                (
                    SELECT id
                    FROM collection_ins
                ),
                (
                    SELECT id
                    FROM collections
                    WHERE name = sqlc.arg('collection')
                )
            )
        ) ON CONFLICT (actor_did, col, rkey) DO
    UPDATE
    SET actor_did = EXCLUDED.actor_did
    RETURNING id
)
INSERT INTO likes (actor_did, rkey, subj, created_at)
SELECT sqlc.arg('actor_did'),
    sqlc.arg('rkey'),
    subject_ins.id,
    sqlc.arg('created_at')
FROM subject_ins;
-- name: DeleteLike :exec
DELETE FROM likes
WHERE actor_did = $1
    AND rkey = $2;
-- name: GetLike :one
SELECT l.*,
    s.actor_did AS subject_actor_did,
    c.name AS subject_namespace,
    s.rkey AS subject_rkey
FROM likes l
    JOIN subjects s ON l.subj = s.id
    JOIN collections c ON s.col = c.id
WHERE l.actor_did = $1
    AND l.rkey = $2
LIMIT 1;
-- name: GetLikesByActor :many
SELECT l.*,
    s.actor_did AS subject_actor_did,
    c.name AS subject_namespace,
    s.rkey AS subject_rkey
FROM likes l
    JOIN subjects s ON l.subj = s.id
    JOIN collections c ON s.col = c.id
WHERE l.actor_did = $1
ORDER BY l.created_at DESC
LIMIT $2 OFFSET $3;
-- name: GetLikesBySubject :many
SELECT l.*,
    s.actor_did AS subject_actor_did,
    c.name AS subject_namespace,
    s.rkey AS subject_rkey
FROM likes l
    JOIN subjects s ON l.subj = s.id
    JOIN collections c ON s.col = c.id
WHERE s.actor_did = $1
    AND c.name = $2
    AND s.rkey = $3
ORDER BY l.created_at DESC
LIMIT $4 OFFSET $5;
-- name: GetTotalLikesReceivedByActor :one
SELECT SUM(num_likes)
FROM like_counts
    JOIN subjects ON like_counts.subject_id = subjects.id
WHERE subjects.actor_did = $1;
-- name: GetTotalLikesGivenByActor :one
SELECT COUNT(*)
FROM likes
WHERE actor_did = $1;
