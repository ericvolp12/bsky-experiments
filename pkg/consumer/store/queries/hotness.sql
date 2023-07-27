-- name: GetHotPage :many
SELECT *
FROM recent_posts_with_score
WHERE score < coalesce(sqlc.narg('score')::float, 100000)
ORDER BY score DESC
LIMIT $1;
