-- name: GetAllUniqueLabels :many
SELECT DISTINCT label
FROM post_labels;
