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
-- name: GetLikesGivenByActorFromTo :one
SELECT COUNT(*)
FROM likes
WHERE actor_did = $1
    AND created_at > sqlc.arg('from')
    AND created_at < sqlc.arg('to');
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
