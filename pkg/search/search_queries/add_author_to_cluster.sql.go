// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.20.0
// source: add_author_to_cluster.sql

package search_queries

import (
	"context"
)

const addAuthorToCluster = `-- name: AddAuthorToCluster :exec
INSERT INTO author_clusters (author_did, cluster_id)
VALUES ($1, $2)
ON CONFLICT (author_did) DO UPDATE SET cluster_id = $2
`

type AddAuthorToClusterParams struct {
	AuthorDid string `json:"author_did"`
	ClusterID int32  `json:"cluster_id"`
}

func (q *Queries) AddAuthorToCluster(ctx context.Context, arg AddAuthorToClusterParams) error {
	_, err := q.exec(ctx, q.addAuthorToClusterStmt, addAuthorToCluster, arg.AuthorDid, arg.ClusterID)
	return err
}
