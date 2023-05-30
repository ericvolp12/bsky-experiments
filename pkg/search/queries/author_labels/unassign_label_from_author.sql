-- name: UnassignLabelFromAuthor :exec
DELETE FROM author_labels
WHERE author_did = sqlc.arg('author_did') AND label_id = sqlc.arg('label_id')::bigint;
