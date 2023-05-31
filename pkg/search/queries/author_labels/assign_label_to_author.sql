-- name: AssignLabelToAuthor :exec
INSERT INTO author_labels (author_did, label_id)
VALUES (sqlc.arg('author_did'), sqlc.arg('label_id')::bigint)
ON CONFLICT (author_did, label_id) DO NOTHING;
