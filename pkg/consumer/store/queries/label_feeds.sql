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
-- name: CreateTQSP :exec
INSERT INTO tqsp(actor_did, rkey)
VALUES ($1, $2);
-- name: DeleteTQSP :exec
DELETE FROM tqsp
WHERE actor_did = $1
    AND rkey = $2;
-- name: GetTQSP :one
SELECT *
FROM tqsp
WHERE actor_did = $1
    AND rkey = $2;
-- name: ListTQSP :many
SELECT *
FROM tqsp
WHERE rkey < $1
ORDER BY rkey DESC
LIMIT $2;
-- name: TrimTQSP :exec
DELETE FROM tqsp
WHERE rkey < $1;
