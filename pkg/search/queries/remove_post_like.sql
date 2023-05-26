-- name: RemoveLikeFromPost :exec
UPDATE post_likes
SET like_count = GREATEST(0, like_count - 1)
WHERE post_id = sqlc.arg('post_id');
