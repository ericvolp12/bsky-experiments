-- name: GetDailySummaries :many
SELECT *
FROM daily_summary;
-- name: GetFollowerPercentiles :one
WITH all_counts AS (
    SELECT a.did,
        COALESCE(fc.num_followers, 0) AS followers
    FROM actors a
        LEFT JOIN follower_counts fc ON a.did = fc.actor_did
),
percentiles AS (
    SELECT percentile_cont(
            ARRAY [0.25, 0.50, 0.75, 0.90, 0.95, 0.99, 0.995, 0.997, 0.999, 0.9999]
        ) WITHIN GROUP (
            ORDER BY followers
        ) AS pct
    FROM all_counts
)
SELECT pct [1]::float AS p25,
    pct [2]::float AS p50,
    pct [3]::float AS p75,
    pct [4]::float AS p90,
    pct [5]::float AS p95,
    pct [6]::float AS p99,
    pct [7]::float AS p99_5,
    pct [8]::float AS p99_7,
    pct [9]::float AS p99_9,
    pct [10]::float AS p99_99
FROM percentiles;
