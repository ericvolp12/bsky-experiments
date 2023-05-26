-- name: GetPostsPageWithAnyLabelSortedByHotness :many
SELECT p.id, p.text, p.parent_post_id, p.root_post_id, p.author_did, p.created_at, 
       p.has_embedded_media, p.parent_relationship, p.sentiment, p.sentiment_confidence,
       (COALESCE(pl.like_count, 0) / EXTRACT(EPOCH FROM NOW() - p.created_at) / 60)::FLOAT AS hotness
FROM posts p
JOIN post_labels ON p.id = post_labels.post_id
LEFT JOIN post_likes pl ON p.id = pl.post_id 
WHERE post_labels.label = ANY(sqlc.arg('labels')::varchar[]) AND 
      (CASE WHEN sqlc.arg('cursor') = '' THEN TRUE ELSE p.id < sqlc.arg('cursor') END) AND
      p.created_at >= NOW() - make_interval(hours := CAST(sqlc.arg('hours_ago') AS INT))
ORDER BY hotness DESC, p.id DESC
LIMIT sqlc.arg('limit');
