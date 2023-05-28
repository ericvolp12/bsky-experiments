-- name: GetBlocksForTarget :many
-- GetBlocksForTarget returns a page of blocks for a given target DID.
-- The blocks are ordered by the created_at timestamp ascending.
SELECT actor_did, target_did, created_at
FROM author_blocks
WHERE target_did = $1
ORDER BY created_at ASC
LIMIT $2
OFFSET $3;
