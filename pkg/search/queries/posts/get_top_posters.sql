-- name: GetTopPosters :many
WITH filtered_authors AS (
    SELECT did,
        handle
    FROM authors
    WHERE did NOT IN (
            'did:plc:jlqiqmhalnu5af3pf56jryei', -- Goose.art's Bot  - intern.goose.art
            'did:plc:vuwg6b5ashezjhh6lpnssljm', -- Spammy Bot       - xnu.kr
            'did:plc:y5smfgzb3oitolqlln3atanl', -- Retroid Bot      - who-up.bsky.social
            'did:plc:czze3j5772nu6gxdhben5i34', -- Berduck          - berduck.deepfates.com
            'did:plc:4hqjfn7m6n5hno3doamuhgef'  -- Yui               - yui.syui.ai
        )
)
SELECT count(id) post_count,
    a.handle,
    posts.author_did
FROM posts
    JOIN authors a on posts.author_did = a.did
WHERE author_did IN (
        SELECT did
        FROM filtered_authors
    )
GROUP BY posts.author_did,
    a.handle
ORDER BY post_count DESC
LIMIT $1;
