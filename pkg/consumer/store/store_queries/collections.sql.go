// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.21.0
// source: collections.sql

package store_queries

import (
	"context"
)

const createCollection = `-- name: CreateCollection :one
INSERT INTO collections (name) VALUES ($1) RETURNING id, name
`

func (q *Queries) CreateCollection(ctx context.Context, name string) (Collection, error) {
	row := q.queryRow(ctx, q.createCollectionStmt, createCollection, name)
	var i Collection
	err := row.Scan(&i.ID, &i.Name)
	return i, err
}

const deleteCollection = `-- name: DeleteCollection :exec
DELETE FROM collections WHERE name = $1
`

func (q *Queries) DeleteCollection(ctx context.Context, name string) error {
	_, err := q.exec(ctx, q.deleteCollectionStmt, deleteCollection, name)
	return err
}

const getCollection = `-- name: GetCollection :one
SELECT id, name FROM collections WHERE name = $1
`

func (q *Queries) GetCollection(ctx context.Context, name string) (Collection, error) {
	row := q.queryRow(ctx, q.getCollectionStmt, getCollection, name)
	var i Collection
	err := row.Scan(&i.ID, &i.Name)
	return i, err
}