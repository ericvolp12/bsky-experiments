// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.27.0
// source: get_cluster_members.sql

package search_queries

import (
	"context"
)

const getMembersOfCluster = `-- name: GetMembersOfCluster :many
SELECT authors.did, authors.handle
FROM authors
JOIN author_clusters ON authors.did = author_clusters.author_did
JOIN clusters ON author_clusters.cluster_id = clusters.id
WHERE clusters.id = $1
`

type GetMembersOfClusterRow struct {
	Did    string `json:"did"`
	Handle string `json:"handle"`
}

func (q *Queries) GetMembersOfCluster(ctx context.Context, clusterID int32) ([]GetMembersOfClusterRow, error) {
	rows, err := q.query(ctx, q.getMembersOfClusterStmt, getMembersOfCluster, clusterID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []GetMembersOfClusterRow
	for rows.Next() {
		var i GetMembersOfClusterRow
		if err := rows.Scan(&i.Did, &i.Handle); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}
