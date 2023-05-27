-- This materialized view is used to calculate the hotness of posts.
-- It is used to speed up custom feed queries that sort by hotness.
CREATE MATERIALIZED VIEW post_hotness AS
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
       post_labels.label,
       (COALESCE(pl.like_count, 0) / GREATEST(1, EXTRACT(EPOCH FROM NOW() - p.created_at) / 60) * EXP(GREATEST(0, (EXTRACT(EPOCH FROM NOW() - p.created_at) / 60 - 360) / 360))) AS hotness
FROM posts p
JOIN post_labels ON p.id = post_labels.post_id
LEFT JOIN post_likes pl ON p.id = pl.post_id
WHERE p.created_at >= NOW() - make_interval(hours := 12) AND
      pl.like_count > 0;
