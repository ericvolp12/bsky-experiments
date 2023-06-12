-- name: GetPostsPageWithAnyPostLabelSortedByHotness :many
SELECT h.id,
       h.text,
       h.parent_post_id,
       h.root_post_id,
       h.author_did,
       h.created_at,
       h.has_embedded_media,
       h.parent_relationship,
       h.sentiment,
       h.sentiment_confidence,
       MAX(h.hotness)::float as hotness
FROM post_hotness h
WHERE h.post_labels && sqlc.arg('labels')::text []
GROUP BY h.id,
       h.text,
       h.parent_post_id,
       h.root_post_id,
       h.author_did,
       h.created_at,
       h.has_embedded_media,
       h.parent_relationship,
       h.sentiment,
       h.sentiment_confidence,
       hotness
HAVING (
              CASE
                     WHEN sqlc.arg('cursor')::float = -1 THEN TRUE
                     ELSE MAX(hotness) < sqlc.arg('cursor')::float
              END
       )
ORDER BY MAX(hotness) DESC,
       h.id DESC
LIMIT sqlc.arg('limit');
