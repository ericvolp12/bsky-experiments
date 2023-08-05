-- name: GetUnindexedPostPage :many
SELECT p.id,
    p.text,
    p.parent_post_id,
    p.root_post_id,
    p.author_did,
    p.created_at,
    p.has_embedded_media,
    p.parent_relationship,
    p.sentiment,
    p.sentiment_confidence,
    p.indexed_at
FROM posts p
WHERE p.indexed_at IS NULL
GROUP BY p.id
ORDER BY p.id
LIMIT $1 OFFSET $2;
