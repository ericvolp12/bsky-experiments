-- name: RemoveAuthorBlock :exec
DELETE FROM author_blocks WHERE actor_did = $1 AND target_did = $2;
