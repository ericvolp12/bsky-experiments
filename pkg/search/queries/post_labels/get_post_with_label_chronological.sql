-- name: GetPostsPageWithPostLabelChronological :many
WITH labeled_posts AS (
    SELECT p.id,
        p.text,
        p.parent_post_id,
        p.root_post_id,
        p.author_did,
        p.created_at,
        p.has_embedded_media,
        p.parent_relationship,
        p.sentiment,
        p.sentiment_confidence
    FROM posts p
        JOIN post_labels l ON p.id = l.post_id
    WHERE l.label = $1
        AND p.created_at < $2
    ORDER BY p.created_at DESC
    LIMIT $3
)
SELECT lp.*,
    (
        SELECT json_agg(l.label) FILTER (
                WHERE l.label IS NOT NULL
            )
        FROM post_labels l
        WHERE l.post_id = lp.id
    ) AS labels
FROM labeled_posts lp;
