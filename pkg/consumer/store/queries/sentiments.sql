-- name: CreateSentimentJob :exec
INSERT INTO post_sentiments (actor_did, rkey, created_at)
VALUES ($1, $2, $3) ON CONFLICT DO NOTHING;
-- name: GetSentimentForPost :one
SELECT *
FROM post_sentiments
WHERE actor_did = $1
    AND rkey = $2;
-- name: GetUnprocessedSentimentJobs :many
SELECT s.*,
    p.content
FROM post_sentiments s
    JOIN posts p ON p.actor_did = s.actor_did
    AND p.rkey = s.rkey
WHERE s.processed_at IS NULL
ORDER BY s.created_at ASC
LIMIT $1 OFFSET $2;
-- name: SetSentimentForPost :exec
INSERT INTO post_sentiments (
        actor_did,
        rkey,
        created_at,
        processed_at,
        sentiment,
        confidence
    )
VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (actor_did, rkey) DO
UPDATE
SET processed_at = $4,
    sentiment = $5,
    confidence = $6;
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
