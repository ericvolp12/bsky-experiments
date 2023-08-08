// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.20.0
// source: posts.sql

package store_queries

import (
	"context"
	"database/sql"
	"time"
)

const createPost = `-- name: CreatePost :exec
INSERT INTO posts (
        actor_did,
        rkey,
        content,
        parent_post_actor_did,
        parent_post_rkey,
        quote_post_actor_did,
        quote_post_rkey,
        root_post_actor_did,
        root_post_rkey,
        has_embedded_media,
        created_at
    )
VALUES (
        $1,
        $2,
        $3,
        $4,
        $5,
        $6,
        $7,
        $8,
        $9,
        $10,
        $11
    )
`

type CreatePostParams struct {
	ActorDid           string         `json:"actor_did"`
	Rkey               string         `json:"rkey"`
	Content            sql.NullString `json:"content"`
	ParentPostActorDid sql.NullString `json:"parent_post_actor_did"`
	ParentPostRkey     sql.NullString `json:"parent_post_rkey"`
	QuotePostActorDid  sql.NullString `json:"quote_post_actor_did"`
	QuotePostRkey      sql.NullString `json:"quote_post_rkey"`
	RootPostActorDid   sql.NullString `json:"root_post_actor_did"`
	RootPostRkey       sql.NullString `json:"root_post_rkey"`
	HasEmbeddedMedia   bool           `json:"has_embedded_media"`
	CreatedAt          sql.NullTime   `json:"created_at"`
}

func (q *Queries) CreatePost(ctx context.Context, arg CreatePostParams) error {
	_, err := q.exec(ctx, q.createPostStmt, createPost,
		arg.ActorDid,
		arg.Rkey,
		arg.Content,
		arg.ParentPostActorDid,
		arg.ParentPostRkey,
		arg.QuotePostActorDid,
		arg.QuotePostRkey,
		arg.RootPostActorDid,
		arg.RootPostRkey,
		arg.HasEmbeddedMedia,
		arg.CreatedAt,
	)
	return err
}

const deletePost = `-- name: DeletePost :exec
DELETE FROM posts
WHERE actor_did = $1
    AND rkey = $2
`

type DeletePostParams struct {
	ActorDid string `json:"actor_did"`
	Rkey     string `json:"rkey"`
}

func (q *Queries) DeletePost(ctx context.Context, arg DeletePostParams) error {
	_, err := q.exec(ctx, q.deletePostStmt, deletePost, arg.ActorDid, arg.Rkey)
	return err
}

const getPost = `-- name: GetPost :one
SELECT actor_did, rkey, content, parent_post_actor_did, quote_post_actor_did, quote_post_rkey, parent_post_rkey, root_post_actor_did, root_post_rkey, has_embedded_media, created_at, inserted_at
FROM posts
WHERE actor_did = $1
    AND rkey = $2
`

type GetPostParams struct {
	ActorDid string `json:"actor_did"`
	Rkey     string `json:"rkey"`
}

func (q *Queries) GetPost(ctx context.Context, arg GetPostParams) (Post, error) {
	row := q.queryRow(ctx, q.getPostStmt, getPost, arg.ActorDid, arg.Rkey)
	var i Post
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
		&i.HasEmbeddedMedia,
		&i.CreatedAt,
		&i.InsertedAt,
	)
	return i, err
}

const getPostsByActor = `-- name: GetPostsByActor :many
SELECT actor_did, rkey, content, parent_post_actor_did, quote_post_actor_did, quote_post_rkey, parent_post_rkey, root_post_actor_did, root_post_rkey, has_embedded_media, created_at, inserted_at
FROM posts
WHERE actor_did = $1
ORDER BY created_at DESC
LIMIT $2
`

type GetPostsByActorParams struct {
	ActorDid string `json:"actor_did"`
	Limit    int32  `json:"limit"`
}

func (q *Queries) GetPostsByActor(ctx context.Context, arg GetPostsByActorParams) ([]Post, error) {
	rows, err := q.query(ctx, q.getPostsByActorStmt, getPostsByActor, arg.ActorDid, arg.Limit)
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

const getPostsByActorsFollowingTarget = `-- name: GetPostsByActorsFollowingTarget :many
WITH followers AS (
    SELECT actor_did
    FROM follows
    WHERE target_did = $1
)
SELECT p.actor_did, p.rkey, p.content, p.parent_post_actor_did, p.quote_post_actor_did, p.quote_post_rkey, p.parent_post_rkey, p.root_post_actor_did, p.root_post_rkey, p.has_embedded_media, p.created_at, p.inserted_at
FROM posts p
    JOIN followers f ON f.actor_did = p.actor_did
WHERE (p.created_at, p.actor_did, p.rkey) < (
        $3::TIMESTAMPTZ,
        $4::TEXT,
        $5::TEXT
    )
    AND (p.root_post_rkey IS NULL)
    AND (
        (p.parent_relationship IS NULL)
        OR (p.parent_relationship <> 'r'::text)
    )
ORDER BY p.created_at DESC,
    p.actor_did DESC,
    p.rkey DESC
LIMIT $2
`

type GetPostsByActorsFollowingTargetParams struct {
	TargetDid       string    `json:"target_did"`
	Limit           int32     `json:"limit"`
	CursorCreatedAt time.Time `json:"cursor_created_at"`
	CursorActorDid  string    `json:"cursor_actor_did"`
	CursorRkey      string    `json:"cursor_rkey"`
}

func (q *Queries) GetPostsByActorsFollowingTarget(ctx context.Context, arg GetPostsByActorsFollowingTargetParams) ([]Post, error) {
	rows, err := q.query(ctx, q.getPostsByActorsFollowingTargetStmt, getPostsByActorsFollowingTarget,
		arg.TargetDid,
		arg.Limit,
		arg.CursorCreatedAt,
		arg.CursorActorDid,
		arg.CursorRkey,
	)
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

const getPostsFromNonMoots = `-- name: GetPostsFromNonMoots :many
WITH my_follows AS (
    SELECT target_did
    FROM follows
    WHERE follows.actor_did = $1
),
non_moots AS (
    SELECT actor_did
    FROM follows f
        LEFT JOIN my_follows ON f.actor_did = my_follows.target_did
    WHERE f.target_did = $1
        AND my_follows.target_did IS NULL
),
non_moots_and_non_spam AS (
    SELECT nm.actor_did
    FROM non_moots nm
        LEFT JOIN following_counts fc ON nm.actor_did = fc.actor_did
    WHERE fc.num_following < 4000
)
SELECT p.actor_did, p.rkey, p.content, p.parent_post_actor_did, p.quote_post_actor_did, p.quote_post_rkey, p.parent_post_rkey, p.root_post_actor_did, p.root_post_rkey, p.has_embedded_media, p.created_at, p.inserted_at
FROM posts p
    JOIN non_moots_and_non_spam f ON f.actor_did = p.actor_did
WHERE (p.created_at, p.actor_did, p.rkey) < (
        $3::TIMESTAMPTZ,
        $4::TEXT,
        $5::TEXT
    )
    AND p.root_post_rkey IS NULL
    AND p.parent_post_rkey IS NULL
    AND p.created_at > NOW() - make_interval(hours := 24)
ORDER BY p.created_at DESC,
    p.actor_did DESC,
    p.rkey DESC
LIMIT $2
`

type GetPostsFromNonMootsParams struct {
	ActorDid        string    `json:"actor_did"`
	Limit           int32     `json:"limit"`
	CursorCreatedAt time.Time `json:"cursor_created_at"`
	CursorActorDid  string    `json:"cursor_actor_did"`
	CursorRkey      string    `json:"cursor_rkey"`
}

func (q *Queries) GetPostsFromNonMoots(ctx context.Context, arg GetPostsFromNonMootsParams) ([]Post, error) {
	rows, err := q.query(ctx, q.getPostsFromNonMootsStmt, getPostsFromNonMoots,
		arg.ActorDid,
		arg.Limit,
		arg.CursorCreatedAt,
		arg.CursorActorDid,
		arg.CursorRkey,
	)
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
