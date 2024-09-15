// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.27.0
// source: update_image.sql

package search_queries

import (
	"context"
	"database/sql"
	"time"

	"github.com/sqlc-dev/pqtype"
)

const updateImage = `-- name: UpdateImage :exec
UPDATE images
SET author_did = $3,
    alt_text = $4,
    mime_type = $5,
    created_at = $6,
    cv_completed = $7,
    cv_run_at = $8,
    cv_classes = $9
WHERE cid = $1
    and post_id = $2
`

type UpdateImageParams struct {
	Cid         string                `json:"cid"`
	PostID      string                `json:"post_id"`
	AuthorDid   string                `json:"author_did"`
	AltText     sql.NullString        `json:"alt_text"`
	MimeType    string                `json:"mime_type"`
	CreatedAt   time.Time             `json:"created_at"`
	CvCompleted bool                  `json:"cv_completed"`
	CvRunAt     sql.NullTime          `json:"cv_run_at"`
	CvClasses   pqtype.NullRawMessage `json:"cv_classes"`
}

func (q *Queries) UpdateImage(ctx context.Context, arg UpdateImageParams) error {
	_, err := q.exec(ctx, q.updateImageStmt, updateImage,
		arg.Cid,
		arg.PostID,
		arg.AuthorDid,
		arg.AltText,
		arg.MimeType,
		arg.CreatedAt,
		arg.CvCompleted,
		arg.CvRunAt,
		arg.CvClasses,
	)
	return err
}
