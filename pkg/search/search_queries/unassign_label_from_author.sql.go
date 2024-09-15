// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.27.0
// source: unassign_label_from_author.sql

package search_queries

import (
	"context"
)

const unassignLabelFromAuthor = `-- name: UnassignLabelFromAuthor :exec
DELETE FROM author_labels
WHERE author_did = $1 AND label_id = $2::bigint
`

type UnassignLabelFromAuthorParams struct {
	AuthorDid string `json:"author_did"`
	LabelID   int64  `json:"label_id"`
}

func (q *Queries) UnassignLabelFromAuthor(ctx context.Context, arg UnassignLabelFromAuthorParams) error {
	_, err := q.exec(ctx, q.unassignLabelFromAuthorStmt, unassignLabelFromAuthor, arg.AuthorDid, arg.LabelID)
	return err
}
