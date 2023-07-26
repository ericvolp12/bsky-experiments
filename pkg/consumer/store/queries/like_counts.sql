-- name: IncrementLikeCountByN :exec
INSERT INTO like_counts (
        actor_did,
        ns,
        rkey,
        num_likes
    )
VALUES (
        $1,
        $2,
        $3,
        $4
    ) ON CONFLICT (actor_did, ns, rkey) DO
UPDATE
SET num_likes = like_counts.num_likes + $4;
-- name: DecrementLikeCountByN :exec
INSERT INTO like_counts (
        actor_did,
        ns,
        rkey,
        num_likes
    )
VALUES (
        $1,
        $2,
        $3,
        $4
    ) ON CONFLICT (actor_did, ns, rkey) DO
UPDATE
SET num_likes = like_counts.num_likes - $4;
-- name: DeleteLikeCount :exec
DELETE FROM like_counts
WHERE actor_did = $1
    AND ns = $2
    AND rkey = $3;
-- name: GetLikeCount :one
SELECT *
FROM like_counts
WHERE actor_did = $1
    AND ns = $2
    AND rkey = $3;
-- name: GetLikeCountsByActor :many
SELECT *
FROM like_counts
WHERE actor_did = $1
ORDER BY updated_at DESC
LIMIT $2;
-- name: GetLikeCountsByActorAndNamespace :many
SELECT *
FROM like_counts
WHERE actor_did = $1
    AND ns = $2
ORDER BY updated_at DESC
LIMIT $3;
