-- name: AddAuthorToCluster :exec
INSERT INTO author_clusters (author_did, cluster_id)
VALUES (sqlc.arg('author_did'), sqlc.arg('cluster_id'))
ON CONFLICT (author_did) DO UPDATE SET cluster_id = sqlc.arg('cluster_id');
