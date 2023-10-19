-- name: GetPostsPageByAuthorLabelAliasFromView :many
SELECT h.*
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

-- name: GetOnlyPostsPageByAuthorLabelAliasFromView :many
SELECT h.*
FROM post_hotness h
WHERE sqlc.arg('lookup_alias') = ANY(h.author_labels)
    AND (
        (h.parent_relationship IS NULL)
        OR (h.parent_relationship <> 'r'::text)
    )
    AND (
        CASE
            WHEN sqlc.arg('cursor') = '' THEN TRUE
            ELSE h.id < sqlc.arg('cursor')
        END
    )
ORDER BY h.id DESC
LIMIT sqlc.arg('limit');
