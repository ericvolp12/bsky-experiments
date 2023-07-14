-- name: GetThreadView :many
WITH RECURSIVE post_tree AS (
       -- Base case: select root post
       SELECT posts.id,
              text,
              parent_post_id,
              root_post_id,
              author_did,
              a2.handle,
              created_at,
              has_embedded_media,
              sentiment,
              sentiment_confidence,
              parent_relationship,
              0 AS depth
       FROM posts
              LEFT JOIN authors a2 on a2.did = posts.author_did
       WHERE posts.id = $1
              AND posts.author_did = $2
       UNION ALL
       -- Recursive case: select child posts
       SELECT p2.id,
              p2.text,
              p2.parent_post_id,
              p2.root_post_id,
              p2.author_did,
              a.handle,
              p2.created_at,
              p2.has_embedded_media,
              p2.sentiment,
              p2.sentiment_confidence,
              p2.parent_relationship,
              pt.depth + 1 AS depth
       FROM posts p2
              JOIN post_tree pt ON p2.parent_post_id = pt.id
              AND p2.parent_relationship != ''
              LEFT JOIN authors a on p2.author_did = a.did
)
SELECT id,
       text,
       parent_post_id,
       root_post_id,
       author_did,
       handle,
       created_at,
       has_embedded_media,
       sentiment,
       sentiment_confidence,
       parent_relationship,
       depth
FROM post_tree
ORDER BY depth;
