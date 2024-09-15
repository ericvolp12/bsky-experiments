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
