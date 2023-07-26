-- name: CreateLike :exec
INSERT INTO likes (
        actor_did,
        rkey,
        subject_actor_did,
        subject_namespace,
        subject_rkey,
        created_at
    )
VALUES (
        $1,
        $2,
        $3,
        $4,
        $5,
        $6
    );
-- name: DeleteLike :exec
DELETE FROM likes
WHERE actor_did = $1
    AND rkey = $2;
-- name: GetLike :one
SELECT *
FROM likes
WHERE actor_did = $1
    AND rkey = $2;
-- name: GetLikesByActor :many
SELECT *
FROM likes
WHERE actor_did = $1
ORDER BY created_at DESC
LIMIT $2;
-- name: GetLikesBySubject :many
SELECT *
FROM likes
WHERE subject_actor_did = $1
    AND subject_namespace = $2
    AND subject_rkey = $3
ORDER BY created_at DESC
LIMIT $4;
