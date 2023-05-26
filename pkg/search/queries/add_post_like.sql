-- name: AddLikeToPost :exec
INSERT INTO post_likes (post_id, like_count)
VALUES (sqlc.arg('post_id'), 1)
ON CONFLICT (post_id)
DO UPDATE SET like_count = post_likes.like_count + 1
WHERE post_likes.post_id = sqlc.arg('post_id');
