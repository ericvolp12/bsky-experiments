-- name: GetTopPosters :many
SELECT *
FROM top_posters
LIMIT $1;
