// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.27.0
// source: sentiments.sql

package store_queries

import (
	"context"
	"database/sql"
	"time"

	"github.com/lib/pq"
	"github.com/sqlc-dev/pqtype"
)

const createSentimentJob = `-- name: CreateSentimentJob :exec
INSERT INTO post_sentiments (actor_did, rkey, created_at)
VALUES ($1, $2, $3) ON CONFLICT DO NOTHING
`

type CreateSentimentJobParams struct {
	ActorDid  string    `json:"actor_did"`
	Rkey      string    `json:"rkey"`
	CreatedAt time.Time `json:"created_at"`
}

func (q *Queries) CreateSentimentJob(ctx context.Context, arg CreateSentimentJobParams) error {
	_, err := q.exec(ctx, q.createSentimentJobStmt, createSentimentJob, arg.ActorDid, arg.Rkey, arg.CreatedAt)
	return err
}

const deleteSentimentJob = `-- name: DeleteSentimentJob :exec
DELETE FROM post_sentiments
WHERE actor_did = $1
    AND rkey = $2
`

type DeleteSentimentJobParams struct {
	ActorDid string `json:"actor_did"`
	Rkey     string `json:"rkey"`
}

func (q *Queries) DeleteSentimentJob(ctx context.Context, arg DeleteSentimentJobParams) error {
	_, err := q.exec(ctx, q.deleteSentimentJobStmt, deleteSentimentJob, arg.ActorDid, arg.Rkey)
	return err
}

const getPostWithSentiment = `-- name: GetPostWithSentiment :one
SELECT p.actor_did, p.rkey, p.content, p.parent_post_actor_did, p.quote_post_actor_did, p.quote_post_rkey, p.parent_post_rkey, p.root_post_actor_did, p.root_post_rkey, p.facets, p.embed, p.langs, p.tags, p.subject_id, p.has_embedded_media, p.created_at, p.inserted_at,
    s.sentiment,
    s.confidence,
    s.processed_at
FROM posts p
    LEFT JOIN post_sentiments s ON s.actor_did = p.actor_did
    AND s.rkey = p.rkey
WHERE p.actor_did = $1
    AND p.rkey = $2
LIMIT 1
`

type GetPostWithSentimentParams struct {
	ActorDid string `json:"actor_did"`
	Rkey     string `json:"rkey"`
}

type GetPostWithSentimentRow struct {
	ActorDid           string                `json:"actor_did"`
	Rkey               string                `json:"rkey"`
	Content            sql.NullString        `json:"content"`
	ParentPostActorDid sql.NullString        `json:"parent_post_actor_did"`
	QuotePostActorDid  sql.NullString        `json:"quote_post_actor_did"`
	QuotePostRkey      sql.NullString        `json:"quote_post_rkey"`
	ParentPostRkey     sql.NullString        `json:"parent_post_rkey"`
	RootPostActorDid   sql.NullString        `json:"root_post_actor_did"`
	RootPostRkey       sql.NullString        `json:"root_post_rkey"`
	Facets             pqtype.NullRawMessage `json:"facets"`
	Embed              pqtype.NullRawMessage `json:"embed"`
	Langs              []string              `json:"langs"`
	Tags               []string              `json:"tags"`
	SubjectID          sql.NullInt64         `json:"subject_id"`
	HasEmbeddedMedia   bool                  `json:"has_embedded_media"`
	CreatedAt          sql.NullTime          `json:"created_at"`
	InsertedAt         time.Time             `json:"inserted_at"`
	Sentiment          sql.NullString        `json:"sentiment"`
	Confidence         sql.NullFloat64       `json:"confidence"`
	ProcessedAt        sql.NullTime          `json:"processed_at"`
}

func (q *Queries) GetPostWithSentiment(ctx context.Context, arg GetPostWithSentimentParams) (GetPostWithSentimentRow, error) {
	row := q.queryRow(ctx, q.getPostWithSentimentStmt, getPostWithSentiment, arg.ActorDid, arg.Rkey)
	var i GetPostWithSentimentRow
	err := row.Scan(
		&i.ActorDid,
		&i.Rkey,
		&i.Content,
		&i.ParentPostActorDid,
		&i.QuotePostActorDid,
		&i.QuotePostRkey,
		&i.ParentPostRkey,
		&i.RootPostActorDid,
		&i.RootPostRkey,
		&i.Facets,
		&i.Embed,
		pq.Array(&i.Langs),
		pq.Array(&i.Tags),
		&i.SubjectID,
		&i.HasEmbeddedMedia,
		&i.CreatedAt,
		&i.InsertedAt,
		&i.Sentiment,
		&i.Confidence,
		&i.ProcessedAt,
	)
	return i, err
}

const getSentimentForPost = `-- name: GetSentimentForPost :one
SELECT actor_did, rkey, inserted_at, created_at, processed_at, sentiment, confidence, detected_langs
FROM post_sentiments
WHERE actor_did = $1
    AND rkey = $2
`

type GetSentimentForPostParams struct {
	ActorDid string `json:"actor_did"`
	Rkey     string `json:"rkey"`
}

func (q *Queries) GetSentimentForPost(ctx context.Context, arg GetSentimentForPostParams) (PostSentiment, error) {
	row := q.queryRow(ctx, q.getSentimentForPostStmt, getSentimentForPost, arg.ActorDid, arg.Rkey)
	var i PostSentiment
	err := row.Scan(
		&i.ActorDid,
		&i.Rkey,
		&i.InsertedAt,
		&i.CreatedAt,
		&i.ProcessedAt,
		&i.Sentiment,
		&i.Confidence,
		pq.Array(&i.DetectedLangs),
	)
	return i, err
}

const getUnprocessedSentimentJobs = `-- name: GetUnprocessedSentimentJobs :many
WITH unprocessed_posts AS (
    SELECT s.actor_did,
        s.rkey
    FROM post_sentiments s
        JOIN posts p ON s.actor_did = p.actor_did
        AND s.rkey = p.rkey
    WHERE s.processed_at IS NULL
    ORDER BY s.created_at
    LIMIT $1
)
SELECT p.actor_did, p.rkey, p.content, p.parent_post_actor_did, p.quote_post_actor_did, p.quote_post_rkey, p.parent_post_rkey, p.root_post_actor_did, p.root_post_rkey, p.facets, p.embed, p.langs, p.tags, p.subject_id, p.has_embedded_media, p.created_at, p.inserted_at
FROM posts p
    JOIN unprocessed_posts s ON p.actor_did = s.actor_did
    AND p.rkey = s.rkey
ORDER BY p.created_at
`

func (q *Queries) GetUnprocessedSentimentJobs(ctx context.Context, limit int32) ([]Post, error) {
	rows, err := q.query(ctx, q.getUnprocessedSentimentJobsStmt, getUnprocessedSentimentJobs, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []Post
	for rows.Next() {
		var i Post
		if err := rows.Scan(
			&i.ActorDid,
			&i.Rkey,
			&i.Content,
			&i.ParentPostActorDid,
			&i.QuotePostActorDid,
			&i.QuotePostRkey,
			&i.ParentPostRkey,
			&i.RootPostActorDid,
			&i.RootPostRkey,
			&i.Facets,
			&i.Embed,
			pq.Array(&i.Langs),
			pq.Array(&i.Tags),
			&i.SubjectID,
			&i.HasEmbeddedMedia,
			&i.CreatedAt,
			&i.InsertedAt,
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

const setSentimentForPost = `-- name: SetSentimentForPost :exec
INSERT INTO post_sentiments (
        actor_did,
        rkey,
        created_at,
        processed_at,
        sentiment,
        confidence,
        detected_langs
    )
VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (actor_did, rkey) DO
UPDATE
SET processed_at = $4,
    sentiment = $5,
    confidence = $6,
    detected_langs = $7
`

type SetSentimentForPostParams struct {
	ActorDid      string          `json:"actor_did"`
	Rkey          string          `json:"rkey"`
	CreatedAt     time.Time       `json:"created_at"`
	ProcessedAt   sql.NullTime    `json:"processed_at"`
	Sentiment     sql.NullString  `json:"sentiment"`
	Confidence    sql.NullFloat64 `json:"confidence"`
	DetectedLangs []string        `json:"detected_langs"`
}

func (q *Queries) SetSentimentForPost(ctx context.Context, arg SetSentimentForPostParams) error {
	_, err := q.exec(ctx, q.setSentimentForPostStmt, setSentimentForPost,
		arg.ActorDid,
		arg.Rkey,
		arg.CreatedAt,
		arg.ProcessedAt,
		arg.Sentiment,
		arg.Confidence,
		pq.Array(arg.DetectedLangs),
	)
	return err
}
