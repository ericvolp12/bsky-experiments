// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.27.0
// source: actor_labels.sql

package store_queries

import (
	"context"
)

const actorHasLabel = `-- name: ActorHasLabel :one
SELECT EXISTS(
        SELECT 1
        FROM actor_labels
        WHERE actor_did = $1
            AND label = $2
    )
`

type ActorHasLabelParams struct {
	ActorDid string `json:"actor_did"`
	Label    string `json:"label"`
}

func (q *Queries) ActorHasLabel(ctx context.Context, arg ActorHasLabelParams) (bool, error) {
	row := q.queryRow(ctx, q.actorHasLabelStmt, actorHasLabel, arg.ActorDid, arg.Label)
	var exists bool
	err := row.Scan(&exists)
	return exists, err
}

const createActorLabel = `-- name: CreateActorLabel :exec
INSERT INTO actor_labels(actor_did, label)
VALUES ($1, $2)
`

type CreateActorLabelParams struct {
	ActorDid string `json:"actor_did"`
	Label    string `json:"label"`
}

func (q *Queries) CreateActorLabel(ctx context.Context, arg CreateActorLabelParams) error {
	_, err := q.exec(ctx, q.createActorLabelStmt, createActorLabel, arg.ActorDid, arg.Label)
	return err
}

const deleteActorLabel = `-- name: DeleteActorLabel :exec
DELETE FROM actor_labels
WHERE actor_did = $1
    AND label = $2
`

type DeleteActorLabelParams struct {
	ActorDid string `json:"actor_did"`
	Label    string `json:"label"`
}

func (q *Queries) DeleteActorLabel(ctx context.Context, arg DeleteActorLabelParams) error {
	_, err := q.exec(ctx, q.deleteActorLabelStmt, deleteActorLabel, arg.ActorDid, arg.Label)
	return err
}

const listActorLabels = `-- name: ListActorLabels :many
SELECT label
FROM actor_labels
WHERE actor_did = $1
ORDER BY label DESC
`

func (q *Queries) ListActorLabels(ctx context.Context, actorDid string) ([]string, error) {
	rows, err := q.query(ctx, q.listActorLabelsStmt, listActorLabels, actorDid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []string
	for rows.Next() {
		var label string
		if err := rows.Scan(&label); err != nil {
			return nil, err
		}
		items = append(items, label)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const listActorsByLabel = `-- name: ListActorsByLabel :many
SELECT actor_did
FROM actor_labels
WHERE label = $1
ORDER BY actor_did DESC
`

func (q *Queries) ListActorsByLabel(ctx context.Context, label string) ([]string, error) {
	rows, err := q.query(ctx, q.listActorsByLabelStmt, listActorsByLabel, label)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []string
	for rows.Next() {
		var actor_did string
		if err := rows.Scan(&actor_did); err != nil {
			return nil, err
		}
		items = append(items, actor_did)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}