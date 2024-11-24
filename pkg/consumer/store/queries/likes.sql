-- name: CreateLike :exec
INSERT INTO likes (actor_did, rkey, subj, created_at)
VALUES (
        sqlc.arg('actor_did'),
        sqlc.arg('rkey'),
        sqlc.arg('subj'),
        sqlc.arg('created_at')
    );
-- name: InsertLike :exec
INSERT INTO likes (actor_did, rkey, subj, created_at)
VALUES ($1, $2, $3, $4);
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
-- name: GetLikesReceivedByActorFromActor :one
SELECT COUNT(*)
FROM likes
    JOIN subjects ON likes.subj = subjects.id
WHERE subjects.actor_did = sqlc.arg('to')
    AND likes.actor_did = sqlc.arg('from');
-- name: GetLikesGivenByActorFromTo :one
WITH p AS MATERIALIZED (
    SELECT p.created_at
    FROM likes l
        JOIN subjects s ON l.subj = s.id
        JOIN posts p ON s.actor_did = p.actor_did
        AND s.rkey = p.rkey
    WHERE l.actor_did = $1
        AND l.created_at > sqlc.arg('from')
        AND l.created_at < sqlc.arg('to')
)
SELECT count(*)
FROM p
WHERE p.created_at > sqlc.arg('from')
    AND p.created_at < sqlc.arg('to');
-- name: FindPotentialFriends :many
WITH user_likes AS (
    SELECT subj
    FROM likes
    WHERE actor_did = $1
        AND created_at > CURRENT_TIMESTAMP - INTERVAL '7 days'
)
SELECT l.actor_did,
    COUNT(l.subj) AS overlap_count
FROM likes l
    JOIN user_likes ul ON l.subj = ul.subj
    LEFT JOIN follows f ON l.actor_did = f.target_did
    AND f.actor_did = $1
    LEFT JOIN blocks b1 ON l.actor_did = b1.target_did
    AND b1.actor_did = $1
    LEFT JOIN blocks b2 ON l.actor_did = b2.actor_did
    AND b2.target_did = $1
WHERE l.actor_did != $1
    AND l.actor_did NOT IN (
        'did:plc:xxno7p4xtpkxtn4ok6prtlcb',
        'did:plc:3tm2l7kcljcgacctmmqru3hj'
    )
    AND l.created_at > CURRENT_TIMESTAMP - INTERVAL '7 days'
    AND f.target_did IS NULL
    AND b1.target_did IS NULL
    AND b2.target_did IS NULL
GROUP BY l.actor_did
ORDER BY overlap_count DESC,
    l.actor_did
LIMIT $2;
