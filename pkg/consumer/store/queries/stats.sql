-- name: GetDailySummaries :many
SELECT *
FROM daily_summary;
-- name: GetFollowerPercentiles :one
SELECT *
FROM follower_stats;
