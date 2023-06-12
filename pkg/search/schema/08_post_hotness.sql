CREATE VIEW post_hotness AS
SELECT p.id,
      p.text,
      p.parent_post_id,
      p.root_post_id,
      p.author_did,
      p.created_at,
      p.has_embedded_media,
      p.parent_relationship,
      p.sentiment,
      p.sentiment_confidence,
      ARRAY_REMOVE(ARRAY_AGG(DISTINCT post_labels.label), NULL) AS post_labels,
      clusters.lookup_alias AS cluster_label,
      ARRAY_REMOVE(ARRAY_AGG(DISTINCT labels.lookup_alias), NULL) AS author_labels,
      (
            COALESCE(pl.like_count, 0) / GREATEST(
                  1,
                  EXTRACT(
                        EPOCH
                        FROM NOW() - p.created_at
                  ) / 60
            ) * EXP(
                  GREATEST(
                        0,
                        (
                              EXTRACT(
                                    EPOCH
                                    FROM NOW() - p.created_at
                              ) / 60 - 360
                        ) / 360
                  )
            )
      )::float AS hotness
FROM posts p
      LEFT JOIN post_labels ON p.id = post_labels.post_id
      LEFT JOIN post_likes pl ON p.id = pl.post_id
      LEFT JOIN author_clusters on p.author_did = author_clusters.author_did
      LEFT JOIN author_labels on p.author_did = author_labels.author_did
      LEFT JOIN labels on author_labels.label_id = labels.id
      LEFT JOIN clusters on author_clusters.cluster_id = clusters.id
WHERE p.created_at >= NOW() - make_interval(hours := 16)
GROUP BY p.id,
      p.text,
      p.parent_post_id,
      p.root_post_id,
      p.author_did,
      p.created_at,
      p.has_embedded_media,
      p.parent_relationship,
      p.sentiment,
      p.sentiment_confidence,
      clusters.lookup_alias,
      pl.like_count;
