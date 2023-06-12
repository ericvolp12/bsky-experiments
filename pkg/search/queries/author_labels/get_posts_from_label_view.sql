-- name: GetPostsPageByAuthorLabelAliasFromView :many
SELECT h.id,
    h.text,
    h.parent_post_id,
    h.root_post_id,
    h.author_did,
    h.created_at,
    h.has_embedded_media,
    h.parent_relationship,
    h.sentiment,
    h.sentiment_confidence
FROM post_hotness h
WHERE sqlc.arg('lookup_alias') = ANY(h.author_labels)
    AND (
        CASE
            WHEN sqlc.arg('cursor') = '' THEN TRUE
            ELSE h.id < sqlc.arg('cursor')
        END
    )
ORDER BY h.id DESC
LIMIT sqlc.arg('limit');
