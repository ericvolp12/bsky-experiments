-- name: GetOldestPresentParent :one
WITH RECURSIVE cte AS (SELECT id,
                              text,
                              parent_post_id,
                              root_post_id,
                              author_did,
                              created_at,
                              has_embedded_media,
                              parent_relationship,
                              sentiment,
                              sentiment_confidence
                       FROM posts
                       WHERE posts.id = $1
                       UNION ALL
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
                                INNER JOIN
                            cte ON p.id = cte.parent_post_id
                       WHERE p.parent_relationship = 'r')
SELECT id,
       text,
       parent_post_id,
       root_post_id,
       author_did,
       created_at,
       has_embedded_media,
       parent_relationship,
       sentiment,
       sentiment_confidence
FROM cte
ORDER BY created_at
LIMIT 1;
