-- name: AddAuthorBlock :exec
INSERT INTO author_blocks (actor_did, target_did, created_at) VALUES ($1, $2, $3) ON CONFLICT (actor_did, target_did) DO UPDATE SET created_at = $3;
