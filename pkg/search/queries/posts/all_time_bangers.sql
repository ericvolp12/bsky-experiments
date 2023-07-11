-- name: GetAllTimeBangers :many
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
    p.indexed_at,
    l.like_count
FROM posts p
    JOIN post_likes l ON l.post_id = p.id
ORDER BY l.like_count DESC
LIMIT sqlc.arg('limit') OFFSET sqlc.arg('offset');
