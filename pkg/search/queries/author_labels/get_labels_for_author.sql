-- name: GetLabelsForAuthor :many
SELECT labels.id, labels.name, labels.lookup_alias
FROM authors
JOIN author_labels ON authors.did = author_labels.author_did
JOIN labels on author_labels.label_id = labels.id
WHERE authors.did = sqlc.arg('author_did');
