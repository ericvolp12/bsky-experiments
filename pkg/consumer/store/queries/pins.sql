-- name: CreatePin :exec
INSERT INTO pins(actor_did, rkey)
VALUES ($1, $2);
-- name: DeletePin :exec
DELETE FROM pins
WHERE actor_did = $1
    AND rkey = $2;
-- name: GetPin :one
SELECT *
FROM pins
WHERE actor_did = $1
    AND rkey = $2;
-- name: ListPinsByActor :many
SELECT *
FROM pins
WHERE actor_did = $1
    AND rkey < $2
ORDER BY rkey DESC
LIMIT $3;
