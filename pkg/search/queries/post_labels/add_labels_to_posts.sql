-- name: AddLabelsToPosts :exec
INSERT INTO post_labels (post_id, author_did, label)
SELECT post_id,
    author_did,
    x.label
FROM jsonb_to_recordset($1::jsonb) AS x(post_id text, author_did text, label text) ON CONFLICT (post_id, label) DO NOTHING;
