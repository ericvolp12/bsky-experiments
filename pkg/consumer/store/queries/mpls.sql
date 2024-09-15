-- name: CreateActorLabel :exec
INSERT INTO actor_labels(actor_did, label)
VALUES ($1, $2);
-- name: DeleteActorLabel :exec
DELETE FROM actor_labels
WHERE actor_did = $1
    AND label = $2;
-- name: ListActorLabels :many
SELECT label
FROM actor_labels
WHERE actor_did = $1
ORDER BY label DESC;
-- name: ActorHasLabel :one
SELECT EXISTS(
        SELECT 1
        FROM actor_labels
        WHERE actor_did = $1
            AND label = $2
    );
-- name: ListActorsByLabel :many
SELECT actor_did
FROM actor_labels
WHERE label = $1
ORDER BY actor_did DESC;
-- name: CreateMPLS :exec
INSERT INTO mpls(actor_did, rkey)
VALUES ($1, $2);
-- name: DeleteMPLS :exec
DELETE FROM mpls
WHERE actor_did = $1
    AND rkey = $2;
-- name: GetMPLS :one
SELECT *
FROM mpls
WHERE actor_did = $1
    AND rkey = $2;
-- name: ListMPLS :many
SELECT *
FROM mpls
WHERE rkey < $1
ORDER BY rkey DESC
LIMIT $2;
-- name: TrimMPLS :exec
DELETE FROM mpls
WHERE rkey < $1;
