-- name: CreateRecentPostLabel :exec
INSERT INTO recent_post_labels(actor_did, rkey, label)
VALUES ($1, $2, $3);
-- name: DeleteRecentPostLabel :exec
DELETE FROM recent_post_labels
WHERE actor_did = $1
    AND rkey = $2
    AND label = $3;
-- name: ListRecentPostLabels :many
SELECT label
FROM recent_post_labels
WHERE actor_did = $1
    AND rkey = $2
ORDER BY label ASC;
-- name: ListRecentPostsByLabelHot :many
SELECT l.actor_did,
    l.rkey
FROM recent_post_labels l
    JOIN recent_posts_with_score rp ON l.actor_did = rp.actor_did
    AND l.rkey = rp.rkey
WHERE label = $1
    AND score < coalesce(sqlc.narg('score')::float, 100000)
ORDER BY score DESC
LIMIT $2;
-- name: TrimRecentPostLabels :exec
DELETE FROM recent_post_labels
WHERE rkey < $1;
