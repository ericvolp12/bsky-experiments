-- name: GetPostsPageByAuthorLabelAlias :many
SELECT p.id, p.text, p.parent_post_id, p.root_post_id, p.author_did, p.created_at, 
       p.has_embedded_media, p.parent_relationship, p.sentiment, p.sentiment_confidence
FROM posts p
JOIN author_labels ON p.author_did = author_labels.author_did
JOIN labels ON author_labels.label_id = labels.id
WHERE labels.lookup_alias = sqlc.arg('lookup_alias') AND 
      (CASE WHEN sqlc.arg('cursor') = '' THEN TRUE ELSE p.id < sqlc.arg('cursor') END) AND
      p.created_at >= NOW() - make_interval(hours := CAST(sqlc.arg('hours_ago') AS INT))
ORDER BY p.id DESC
LIMIT sqlc.arg('limit');
