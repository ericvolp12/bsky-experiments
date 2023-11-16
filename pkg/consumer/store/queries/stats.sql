-- name: GetDailySummaries :many
SELECT *
FROM daily_summary
ORDER BY date DESC;
-- name: GetFollowerPercentiles :one
SELECT *
FROM follower_stats;
