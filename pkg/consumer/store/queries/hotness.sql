-- name: GetHotPage :many
SELECT *
FROM recent_posts_with_score rp
    JOIN following_counts fc ON rp.actor_did = fc.actor_did
WHERE score < coalesce(sqlc.narg('score')::float, 100000)
    AND fc.num_following < 4000
ORDER BY score DESC
LIMIT $1;
