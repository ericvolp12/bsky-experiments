// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.27.0
// source: outliers.sql

package store_queries

import (
	"context"
	"time"
)

const insertFollowerOutliers = `-- name: InsertFollowerOutliers :exec
INSERT INTO follower_outliers (
        subject,
        num_followers,
        period,
        created_at
    )
VALUES ($1, $2, $3, $4)
`

type InsertFollowerOutliersParams struct {
	Subject      string    `json:"subject"`
	NumFollowers int64     `json:"num_followers"`
	Period       int64     `json:"period"`
	CreatedAt    time.Time `json:"created_at"`
}

func (q *Queries) InsertFollowerOutliers(ctx context.Context, arg InsertFollowerOutliersParams) error {
	_, err := q.exec(ctx, q.insertFollowerOutliersStmt, insertFollowerOutliers,
		arg.Subject,
		arg.NumFollowers,
		arg.Period,
		arg.CreatedAt,
	)
	return err
}

const insertLikeOutliers = `-- name: InsertLikeOutliers :exec
INSERT INTO like_outliers (
        subject,
        num_likes,
        period,
        created_at
    )
VALUES ($1, $2, $3, $4)
`

type InsertLikeOutliersParams struct {
	Subject   string    `json:"subject"`
	NumLikes  int64     `json:"num_likes"`
	Period    int64     `json:"period"`
	CreatedAt time.Time `json:"created_at"`
}

func (q *Queries) InsertLikeOutliers(ctx context.Context, arg InsertLikeOutliersParams) error {
	_, err := q.exec(ctx, q.insertLikeOutliersStmt, insertLikeOutliers,
		arg.Subject,
		arg.NumLikes,
		arg.Period,
		arg.CreatedAt,
	)
	return err
}

const insertOperationOutliers = `-- name: InsertOperationOutliers :exec
INSERT INTO operation_outliers (
        actor_did,
        collection,
        operation,
        num_ops,
        period,
        created_at
    )
VALUES ($1, $2, $3, $4, $5, $6)
`

type InsertOperationOutliersParams struct {
	ActorDid   string    `json:"actor_did"`
	Collection string    `json:"collection"`
	Operation  string    `json:"operation"`
	NumOps     int64     `json:"num_ops"`
	Period     int64     `json:"period"`
	CreatedAt  time.Time `json:"created_at"`
}

// CREATE TABLE operation_outliers (
//
//	id BIGSERIAL PRIMARY KEY,
//	actor_did TEXT NOT NULL,
//	collection TEXT NOT NULL,
//	operation TEXT NOT NULL,
//	num_ops BIGINT NOT NULL,
//	period TEXT NOT NULL,
//	created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL
//
// );
// CREATE TABLE follower_outliers (
//
//	id BIGSERIAL PRIMARY KEY,
//	subject TEXT NOT NULL,
//	num_followers BIGINT NOT NULL,
//	period TEXT NOT NULL,
//	created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL
//
// );
// CREATE TABLE like_outliers (
//
//	id BIGSERIAL PRIMARY KEY,
//	subject TEXT NOT NULL,
//	num_likes BIGINT NOT NULL,
//	period TEXT NOT NULL,
//	created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL
//
// );
func (q *Queries) InsertOperationOutliers(ctx context.Context, arg InsertOperationOutliersParams) error {
	_, err := q.exec(ctx, q.insertOperationOutliersStmt, insertOperationOutliers,
		arg.ActorDid,
		arg.Collection,
		arg.Operation,
		arg.NumOps,
		arg.Period,
		arg.CreatedAt,
	)
	return err
}
