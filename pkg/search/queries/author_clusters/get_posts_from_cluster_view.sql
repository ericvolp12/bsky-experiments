-- name: GetPostsPageByClusterAliasFromView :many
SELECT DISTINCT ON (h.id) h.id,
    h.text,
    h.parent_post_id,
    h.root_post_id,
    h.author_did,
    h.created_at,
    h.has_embedded_media,
    h.parent_relationship,
    h.sentiment,
    h.sentiment_confidence,
    h.hotness::float as hotness
FROM post_hotness h
WHERE h.cluster_label = $1
    AND h.created_at < $2
ORDER BY h.created_at DESC
LIMIT sqlc.arg('limit');
