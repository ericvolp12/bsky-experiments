-- name: GetMembersOfCluster :many
SELECT authors.did, authors.handle
FROM authors
JOIN author_clusters ON authors.did = author_clusters.author_did
JOIN clusters ON author_clusters.cluster_id = clusters.id
WHERE clusters.id = sqlc.arg('cluster_id');
