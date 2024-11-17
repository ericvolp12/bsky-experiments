-- name: CreateRecentPostLabel :exec
INSERT INTO recent_post_labels(actor_did, rkey, label, subject_id)
VALUES ($1, $2, $3, $4);
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
WITH filtered_posts AS (
    SELECT subject_id,
        score
    FROM recent_posts_with_score
    WHERE score < coalesce(sqlc.narg('score')::float, 100000)
)
SELECT l.actor_did,
    l.rkey,
    fp.score
FROM filtered_posts fp
    JOIN recent_post_labels l ON l.subject_id = fp.subject_id
WHERE l.label = $1
ORDER BY fp.score DESC
LIMIT $2;
-- name: TrimRecentPostLabels :exec
DELETE FROM recent_post_labels
WHERE rkey < $1;
