-- name: GetPostsPageWithLabel :many
SELECT p.id, p.text, p.parent_post_id, p.root_post_id, p.author_did, p.created_at, 
       p.has_embedded_media, p.parent_relationship, p.sentiment, p.sentiment_confidence
FROM posts p
JOIN post_labels ON p.id = post_labels.post_id 
WHERE post_labels.label = sqlc.arg('label') AND 
      (CASE WHEN sqlc.arg('cursor') = '' THEN TRUE ELSE p.id < sqlc.arg('cursor') END) AND
      p.created_at >= NOW() - make_interval(hours := CAST(sqlc.arg('hours_ago') AS INT))
ORDER BY p.id DESC
LIMIT sqlc.arg('limit');
