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
