-- Follow Counts
-- name: IncrementFollowerCountByN :exec
INSERT INTO follower_counts (actor_did, num_followers, updated_at)
SELECT $1,
    sqlc.arg('num_followers')::int,
    $2 ON CONFLICT (actor_did) DO
UPDATE
SET (num_followers, updated_at) = (
        follower_counts.num_followers + EXCLUDED.num_followers,
        EXCLUDED.updated_at
    );
-- name: DecrementFollowerCountByN :exec
INSERT INTO follower_counts (actor_did, num_followers, updated_at)
SELECT $1,
    - sqlc.arg('num_followers')::int,
    $2 ON CONFLICT (actor_did) DO
UPDATE
SET (num_followers, updated_at) = (
        GREATEST(
            0,
            follower_counts.num_followers + EXCLUDED.num_followers
        ),
        EXCLUDED.updated_at
    );
-- name: DeleteFollowerCount :exec
DELETE FROM follower_counts
WHERE actor_did = $1;
-- name: GetFollowerCount :one
SELECT nfc.*
FROM follower_counts nfc
WHERE nfc.actor_did = $1;
-- Following Counts
-- name: IncrementFollowingCountByN :exec
INSERT INTO following_counts (actor_did, num_following, updated_at)
SELECT $1,
    sqlc.arg('num_following')::int,
    $2 ON CONFLICT (actor_did) DO
UPDATE
SET (num_following, updated_at) = (
        following_counts.num_following + EXCLUDED.num_following,
        EXCLUDED.updated_at
    );
-- name: DecrementFollowingCountByN :exec
INSERT INTO following_counts (actor_did, num_following, updated_at)
SELECT $1,
    - sqlc.arg('num_following')::int,
    $2 ON CONFLICT (actor_did) DO
UPDATE
SET (num_following, updated_at) = (
        GREATEST(
            0,
            following_counts.num_following + EXCLUDED.num_following
        ),
        EXCLUDED.updated_at
    );
-- name: DeleteFollowingCount :exec
DELETE FROM following_counts
WHERE actor_did = $1;
-- name: GetFollowingCount :one
SELECT nfc.*
FROM following_counts nfc
WHERE nfc.actor_did = $1;
