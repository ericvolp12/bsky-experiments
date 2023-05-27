-- name: GetPostsPageWithAnyLabelSortedByHotness :many
SELECT h.id, h.text, h.parent_post_id, h.root_post_id, h.author_did, h.created_at, 
       h.has_embedded_media, h.parent_relationship, h.sentiment, h.sentiment_confidence, MAX(h.hotness) as hotness
FROM post_hotness h
WHERE h.label = ANY(sqlc.arg('labels')::varchar[])
GROUP BY h.id, h.text, h.parent_post_id, h.root_post_id, h.author_did, h.created_at, 
         h.has_embedded_media, h.parent_relationship, h.sentiment, h.sentiment_confidence
HAVING (CASE WHEN sqlc.arg('cursor') = '' THEN TRUE ELSE h.id < sqlc.arg('cursor') END)
ORDER BY hotness DESC, h.id DESC
LIMIT sqlc.arg('limit');
