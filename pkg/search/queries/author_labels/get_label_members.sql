-- name: GetMembersOfAuthorLabel :many
SELECT authors.did, authors.handle
FROM authors
JOIN author_labels ON authors.did = author_labels.author_did
JOIN labels ON author_labels.label_id = labels.id
WHERE labels.id = sqlc.arg('label_id');
