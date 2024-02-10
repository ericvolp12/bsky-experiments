-- name: UpsertActor :exec
INSERT INTO actors (
        did,
        handle,
        created_at,
        updated_at
    )
VALUES ($1, $2, $3, $3) ON CONFLICT (did) DO
UPDATE
SET handle = EXCLUDED.handle,
    updated_at = EXCLUDED.updated_at
WHERE actors.did = EXCLUDED.did;
-- name: UpsertActorFromFirehose :exec
INSERT INTO actors (
        did,
        handle,
        display_name,
        bio,
        pro_pic_cid,
        banner_cid,
        created_at,
        updated_at
    )
VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT (did) DO
UPDATE
SET display_name = EXCLUDED.display_name,
    bio = EXCLUDED.bio,
    pro_pic_cid = EXCLUDED.pro_pic_cid,
    banner_cid = EXCLUDED.banner_cid,
    updated_at = EXCLUDED.updated_at
WHERE actors.did = EXCLUDED.did;
-- name: GetActorByDID :one
SELECT *
FROM actors
WHERE did = $1;
-- name: GetActorByHandle :one
SELECT *
FROM actors
WHERE handle = $1;
-- name: FindActorsByHandle :many
SELECT *
FROM actors
WHERE handle ILIKE concat('%', $1, '%');
-- name: GetActorTypeAhead :many
SELECT did,
    handle,
    actors.created_at,
    CASE
        WHEN f.actor_did IS NOT NULL
        AND f2.actor_did IS NOT NULL THEN similarity(
            handle,
            concat('%', sqlc.arg('query')::text, '%')
        ) + 1
        WHEN f.actor_did IS NOT NULL THEN similarity(
            handle,
            concat('%', sqlc.arg('query')::text, '%')
        ) + 0.5
        ELSE similarity(
            handle,
            concat('%', sqlc.arg('query')::text, '%')
        )
    END::float AS score
FROM actors
    LEFT JOIN follows f ON f.target_did = did
    AND f.actor_did = sqlc.arg('actor_did')
    LEFT JOIN follows f2 ON f2.target_did = sqlc.arg('actor_did')
    AND f2.actor_did = did
WHERE handle ilike concat('%', sqlc.arg('query')::text, '%')
    AND NOT EXISTS (
        SELECT 1
        FROM blocks b
        WHERE (
                b.actor_did = sqlc.arg('actor_did')
                AND b.target_did = did
            )
            OR (
                b.actor_did = did
                AND b.target_did = sqlc.arg('actor_did')
            )
    )
ORDER BY score DESC
LIMIT $1;
-- name: GetActorsWithoutPropic :many
SELECT *
FROM actors
WHERE pro_pic_cid IS NULL
LIMIT $1;
-- name: UpdateActorPropic :exec
UPDATE actors
SET pro_pic_cid = $2,
    updated_at = $3
WHERE did = $1;
-- name: GetActorsForValidation :many
SELECT *
from actors
WHERE last_validated is NULL
    OR last_validated < $1
ORDER BY did
LIMIT $2;
-- name: UpdateActorsValidation :exec
UPDATE actors
SET last_validated = $1,
    handle_valid = $2
WHERE did = ANY(sqlc.arg('dids')::text []);
-- name: GetSpamFollowers :many
SELECT actor_did
FROM following_counts fc
WHERE fc.num_following > 4000;
