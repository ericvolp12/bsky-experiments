-- name: SetPostSentiment :exec
UPDATE posts
SET sentiment = $1,
    sentiment_confidence = $2
WHERE id = $3
    AND author_did = $4;
