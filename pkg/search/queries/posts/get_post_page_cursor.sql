-- name: GetPostPageCursor :many
WITH filtered_posts AS (
    SELECT *
    FROM posts
    WHERE created_at < $1
    ORDER BY created_at DESC
    LIMIT $2
)
SELECT fp.id,
    fp.text,
    fp.parent_post_id,
    fp.root_post_id,
    fp.author_did,
    fp.created_at,
    fp.has_embedded_media,
    fp.parent_relationship,
    fp.sentiment,
    fp.sentiment_confidence,
    fp.indexed_at,
    (
        SELECT COALESCE(
                json_agg(l.label) FILTER (
                    WHERE l.label IS NOT NULL
                ),
                '[]'
            )
        FROM post_labels l
        WHERE l.post_id = fp.id
    ) as labels
FROM filtered_posts fp;
