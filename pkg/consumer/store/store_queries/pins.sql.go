// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.27.0
// source: pins.sql

package store_queries

import (
	"context"
)

const createPin = `-- name: CreatePin :exec
INSERT INTO pins(actor_did, rkey)
VALUES ($1, $2)
`

type CreatePinParams struct {
	ActorDid string `json:"actor_did"`
	Rkey     string `json:"rkey"`
}

func (q *Queries) CreatePin(ctx context.Context, arg CreatePinParams) error {
	_, err := q.exec(ctx, q.createPinStmt, createPin, arg.ActorDid, arg.Rkey)
	return err
}

const deletePin = `-- name: DeletePin :exec
DELETE FROM pins
WHERE actor_did = $1
    AND rkey = $2
`

type DeletePinParams struct {
	ActorDid string `json:"actor_did"`
	Rkey     string `json:"rkey"`
}

func (q *Queries) DeletePin(ctx context.Context, arg DeletePinParams) error {
	_, err := q.exec(ctx, q.deletePinStmt, deletePin, arg.ActorDid, arg.Rkey)
	return err
}

const getPin = `-- name: GetPin :one
SELECT actor_did, rkey
FROM pins
WHERE actor_did = $1
    AND rkey = $2
`

type GetPinParams struct {
	ActorDid string `json:"actor_did"`
	Rkey     string `json:"rkey"`
}

func (q *Queries) GetPin(ctx context.Context, arg GetPinParams) (Pin, error) {
	row := q.queryRow(ctx, q.getPinStmt, getPin, arg.ActorDid, arg.Rkey)
	var i Pin
	err := row.Scan(&i.ActorDid, &i.Rkey)
	return i, err
}

const listPinsByActor = `-- name: ListPinsByActor :many
SELECT actor_did, rkey
FROM pins
WHERE actor_did = $1
    AND rkey < $2
ORDER BY rkey DESC
LIMIT $3
`

type ListPinsByActorParams struct {
	ActorDid string `json:"actor_did"`
	Rkey     string `json:"rkey"`
	Limit    int32  `json:"limit"`
}

func (q *Queries) ListPinsByActor(ctx context.Context, arg ListPinsByActorParams) ([]Pin, error) {
	rows, err := q.query(ctx, q.listPinsByActorStmt, listPinsByActor, arg.ActorDid, arg.Rkey, arg.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []Pin
	for rows.Next() {
		var i Pin
		if err := rows.Scan(&i.ActorDid, &i.Rkey); err != nil {
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
