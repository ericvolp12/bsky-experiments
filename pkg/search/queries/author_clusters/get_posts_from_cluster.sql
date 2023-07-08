-- name: GetPostsPageByClusterAlias :many
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
      JOIN author_clusters ON p.author_did = author_clusters.author_did
      JOIN clusters ON author_clusters.cluster_id = clusters.id
WHERE clusters.lookup_alias = $1
      AND p.created_at < $2
      AND p.created_at >= NOW() - make_interval(hours := CAST(sqlc.arg('hours_ago') AS INT))
ORDER BY p.created_at DESC
LIMIT $3;
