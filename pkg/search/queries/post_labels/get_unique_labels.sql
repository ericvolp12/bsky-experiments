-- name: GetAllUniquePostLabels :many
SELECT DISTINCT label
FROM post_labels;
