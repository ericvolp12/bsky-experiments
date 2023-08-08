// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.20.0
// source: hotness.sql

package store_queries

import (
	"context"
	"database/sql"
	"time"
)

const getHotPage = `-- name: GetHotPage :many
SELECT rp.actor_did, subject_id, rkey, created_at, inserted_at, score, fc.actor_did, num_following, updated_at
FROM recent_posts_with_score rp
    JOIN following_counts fc ON rp.actor_did = fc.actor_did
WHERE score < coalesce($2::float, 100000)
    AND fc.num_following < 4000
ORDER BY score DESC
LIMIT $1
`

type GetHotPageParams struct {
	Limit int32           `json:"limit"`
	Score sql.NullFloat64 `json:"score"`
}

type GetHotPageRow struct {
	ActorDid     string       `json:"actor_did"`
	SubjectID    int64        `json:"subject_id"`
	Rkey         string       `json:"rkey"`
	CreatedAt    sql.NullTime `json:"created_at"`
	InsertedAt   interface{}  `json:"inserted_at"`
	Score        float64      `json:"score"`
	ActorDid_2   string       `json:"actor_did_2"`
	NumFollowing int64        `json:"num_following"`
	UpdatedAt    time.Time    `json:"updated_at"`
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
			&i.ActorDid,
			&i.SubjectID,
			&i.Rkey,
			&i.CreatedAt,
			&i.InsertedAt,
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
