-- name: GetPostPageCursor :many
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
    COALESCE(
        json_agg(l.label) FILTER (
            WHERE l.label IS NOT NULL
        ),
        '[]'
    ) as labels
FROM posts p
    LEFT JOIN post_labels l on l.post_id = p.id
WHERE CASE
        WHEN sqlc.arg('cursor') = '' THEN TRUE
        ELSE p.id < sqlc.arg('cursor')
    END
GROUP BY p.id
ORDER BY p.id DESC
LIMIT $1;
