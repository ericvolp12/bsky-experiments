// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.27.0
// source: hotness.sql

package store_queries

import (
	"context"
	"database/sql"
	"time"

	"github.com/lib/pq"
)

const getHotPage = `-- name: GetHotPage :many
SELECT subject_id, rp.actor_did, rkey, subject_created_at, inserted_at, langs, has_embedded_media, score, fc.actor_did, num_following, updated_at
FROM recent_posts_with_score rp
    LEFT JOIN following_counts fc ON rp.actor_did = fc.actor_did
WHERE score < coalesce($2::float, 100000)
    AND (
        fc.num_following < 4000
        OR num_following is NULL
    )
ORDER BY score DESC
LIMIT $1
`

type GetHotPageParams struct {
	Limit int32           `json:"limit"`
	Score sql.NullFloat64 `json:"score"`
}

type GetHotPageRow struct {
	SubjectID        int64          `json:"subject_id"`
	ActorDid         string         `json:"actor_did"`
	Rkey             string         `json:"rkey"`
	SubjectCreatedAt sql.NullTime   `json:"subject_created_at"`
	InsertedAt       time.Time      `json:"inserted_at"`
	Langs            []string       `json:"langs"`
	HasEmbeddedMedia bool           `json:"has_embedded_media"`
	Score            float64        `json:"score"`
	ActorDid_2       sql.NullString `json:"actor_did_2"`
	NumFollowing     sql.NullInt64  `json:"num_following"`
	UpdatedAt        sql.NullTime   `json:"updated_at"`
}

func (q *Queries) GetHotPage(ctx context.Context, arg GetHotPageParams) ([]GetHotPageRow, error) {
	rows, err := q.query(ctx, q.getHotPageStmt, getHotPage, arg.Limit, arg.Score)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []GetHotPageRow
	for rows.Next() {
		var i GetHotPageRow
		if err := rows.Scan(
			&i.SubjectID,
			&i.ActorDid,
			&i.Rkey,
			&i.SubjectCreatedAt,
			&i.InsertedAt,
			pq.Array(&i.Langs),
			&i.HasEmbeddedMedia,
			&i.Score,
			&i.ActorDid_2,
			&i.NumFollowing,
			&i.UpdatedAt,
		); err != nil {
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
