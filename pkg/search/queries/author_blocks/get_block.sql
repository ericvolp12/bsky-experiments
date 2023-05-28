-- name: GetAuthorBlock :one
SELECT actor_did, target_did, created_at
FROM author_blocks
WHERE actor_did = $1 AND target_did = $2;
