-- name: GetBlockedByCountForTarget :one
SELECT COUNT(*) AS count
FROM author_blocks
WHERE target_did = $1;
