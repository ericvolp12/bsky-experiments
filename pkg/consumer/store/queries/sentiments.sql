-- name: CreateSentimentJob :exec
INSERT INTO post_sentiments (actor_did, rkey, created_at)
VALUES ($1, $2, $3) ON CONFLICT DO NOTHING;
-- name: GetSentimentForPost :one
SELECT *
FROM post_sentiments
WHERE actor_did = $1
    AND rkey = $2;
-- name: GetUnprocessedSentimentJobs :many
WITH unprocessed_posts AS (
    SELECT s.actor_did,
        s.rkey
    FROM post_sentiments s
    WHERE s.processed_at IS NULL
    ORDER BY s.created_at
    LIMIT $1
)
SELECT p.*
FROM posts p
    JOIN unprocessed_posts s ON p.actor_did = s.actor_did
    AND p.rkey = s.rkey
ORDER BY p.created_at;
-- name: SetSentimentForPost :exec
INSERT INTO post_sentiments (
        actor_did,
        rkey,
        created_at,
        processed_at,
        sentiment,
        confidence,
        detected_langs
    )
VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (actor_did, rkey) DO
UPDATE
SET processed_at = $4,
    sentiment = $5,
    confidence = $6,
    detected_langs = $7;
-- name: DeleteSentimentJob :exec
DELETE FROM post_sentiments
WHERE actor_did = $1
    AND rkey = $2;
-- name: GetPostWithSentiment :one
SELECT p.*,
    s.sentiment,
    s.confidence,
    s.processed_at
FROM posts p
    LEFT JOIN post_sentiments s ON s.actor_did = p.actor_did
    AND s.rkey = p.rkey
WHERE p.actor_did = $1
    AND p.rkey = $2
LIMIT 1;
