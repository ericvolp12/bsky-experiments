-- name: GetPostsPageWithLabelSortedByHotness :many
SELECT h.id, h.text, h.parent_post_id, h.root_post_id, h.author_did, h.created_at, 
       h.has_embedded_media, h.parent_relationship, h.sentiment, h.sentiment_confidence, h.hotness
FROM post_hotness h
WHERE h.label = sqlc.arg('label') AND 
      (CASE WHEN sqlc.arg('cursor') = '' THEN TRUE ELSE h.id < sqlc.arg('cursor') END)
ORDER BY h.hotness DESC, h.id DESC
LIMIT sqlc.arg('limit');
