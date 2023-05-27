-- name: GetTopPosters :many
WITH filtered_authors AS (
    SELECT did, handle
    FROM authors
    WHERE handle NOT IN (
        'intern.goose.art', 'xnu.kr', 'who-up.bsky.social',
        'yui.bsky.social', 'berduck.deepfates.com'
    )
)

SELECT count(id)  post_count, a.handle, posts.author_did
    FROM posts JOIN authors a on posts.author_did = a.did
    WHERE author_did IN (SELECT did FROM filtered_authors)
    GROUP BY posts.author_did, a.handle
    ORDER BY post_count DESC
    LIMIT $1;
