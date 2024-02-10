-- name: CreatePost :exec
INSERT INTO posts (
        actor_did,
        rkey,
        content,
        parent_post_actor_did,
        parent_post_rkey,
        quote_post_actor_did,
        quote_post_rkey,
        root_post_actor_did,
        root_post_rkey,
        has_embedded_media,
        facets,
        embed,
        langs,
        tags,
        created_at,
        inserted_at
    )
VALUES (
        $1,
        $2,
        $3,
        $4,
        $5,
        $6,
        $7,
        $8,
        $9,
        $10,
        $11,
        $12,
        $13,
        $14,
        $15,
        $16
    );
-- name: DeletePost :exec
DELETE FROM posts
WHERE actor_did = $1
    AND rkey = $2;
-- name: GetPost :one
SELECT *
FROM posts
WHERE actor_did = $1
    AND rkey = $2;
-- name: GetPostsByActor :many
SELECT *
FROM posts
WHERE actor_did = $1
ORDER BY created_at DESC
LIMIT $2;
-- name: GetPinnedPostsByActor :many
SELECT *
FROM posts
WHERE actor_did = $1
    AND (
        content LIKE '%📌%'
        OR content LIKE '%🔖%'
    )
    AND parent_post_rkey IS NOT NULL
    AND parent_post_actor_did IS NOT NULL
ORDER BY inserted_at DESC
LIMIT $2 OFFSET $3;
-- name: GetPostsByActorsFollowingTarget :many
WITH followers AS (
    SELECT actor_did
    FROM follows
    WHERE target_did = $1
)
SELECT p.*
FROM posts p
    JOIN followers f ON f.actor_did = p.actor_did
WHERE (p.created_at, p.actor_did, p.rkey) < (
        sqlc.arg('cursor_created_at')::TIMESTAMPTZ,
        sqlc.arg('cursor_actor_did')::TEXT,
        sqlc.arg('cursor_rkey')::TEXT
    )
    AND (p.root_post_rkey IS NULL)
    AND (
        (p.parent_relationship IS NULL)
        OR (p.parent_relationship <> 'r'::text)
    )
ORDER BY p.created_at DESC,
    p.actor_did DESC,
    p.rkey DESC
LIMIT $2;
-- name: GetPostsFromNonSpamUsers :many
WITH non_spam AS (
    SELECT nm.actor_did
    FROM unnest(sqlc.arg('dids')::TEXT []) nm(actor_did)
        LEFT JOIN following_counts fc ON nm.actor_did = fc.actor_did
    WHERE fc.num_following < 4000
)
SELECT p.*
FROM posts p
    JOIN non_spam f ON f.actor_did = p.actor_did
WHERE (p.created_at, p.actor_did, p.rkey) < (
        sqlc.arg('cursor_created_at')::TIMESTAMPTZ,
        sqlc.arg('cursor_actor_did')::TEXT,
        sqlc.arg('cursor_rkey')::TEXT
    )
    AND p.root_post_rkey IS NULL
    AND p.parent_post_rkey IS NULL
    AND p.created_at > NOW() - make_interval(hours := 24)
ORDER BY p.created_at DESC,
    p.actor_did DESC,
    p.rkey DESC
LIMIT $1;
-- name: GetPostsFromNonMoots :many
WITH my_follows AS (
    SELECT target_did
    FROM follows
    WHERE follows.actor_did = $1
),
non_moots AS (
    SELECT actor_did
    FROM follows f
        LEFT JOIN my_follows ON f.actor_did = my_follows.target_did
    WHERE f.target_did = $1
        AND my_follows.target_did IS NULL
),
non_moots_and_non_spam AS (
    SELECT nm.actor_did
    FROM non_moots nm
        LEFT JOIN following_counts fc ON nm.actor_did = fc.actor_did
    WHERE fc.num_following < 4000
)
SELECT p.*
FROM posts p
    JOIN non_moots_and_non_spam f ON f.actor_did = p.actor_did
WHERE (p.created_at, p.actor_did, p.rkey) < (
        sqlc.arg('cursor_created_at')::TIMESTAMPTZ,
        sqlc.arg('cursor_actor_did')::TEXT,
        sqlc.arg('cursor_rkey')::TEXT
    )
    AND p.root_post_rkey IS NULL
    AND p.parent_post_rkey IS NULL
    AND p.created_at > NOW() - make_interval(hours := 24)
ORDER BY p.created_at DESC,
    p.actor_did DESC,
    p.rkey DESC
LIMIT $2;
-- name: GetMyPostsByFuzzyContent :many
SELECT *
FROM posts
WHERE actor_did = $1
    AND content ILIKE concat('%', sqlc.arg('query')::text, '%')::text
    AND content not ilike '%!jazbot%'
ORDER BY created_at DESC
LIMIT $2 OFFSET $3;
-- name: GetTopPosts :many
WITH TopSubjects AS (
    SELECT s.*,
        lc.num_likes
    FROM subjects s
        JOIN like_counts lc ON lc.subject_id = s.id
    WHERE s.col = 1
        AND lc.num_likes > 100
    ORDER BY lc.num_likes DESC
    LIMIT $1 + 30 OFFSET $2
)
SELECT p.*
FROM posts p
    JOIN TopSubjects s ON p.actor_did = s.actor_did
    AND p.rkey = s.rkey
ORDER BY s.num_likes DESC
LIMIT $1;
-- name: GetTopPostsForActor :many
WITH TopSubjects AS (
    SELECT s.*,
        lc.num_likes
    FROM subjects s
        JOIN like_counts lc ON lc.subject_id = s.id
    WHERE s.col = 1
        AND lc.num_likes > 1
        AND s.actor_did = $1
    ORDER BY lc.num_likes DESC
    LIMIT $2 OFFSET $3
)
SELECT p.*
FROM posts p
    JOIN TopSubjects s ON p.actor_did = s.actor_did
    AND p.rkey = s.rkey
ORDER BY s.num_likes DESC;
-- name: GetPostWithReplies :many
WITH RootPost AS (
    SELECT p.*,
        a.handle,
        a.pro_pic_cid,
        array_agg(COALESCE(i.cid, ''))::TEXT [] as image_cids,
        array_agg(COALESCE(i.alt_text, ''))::TEXT [] as image_alts
    FROM posts p
        LEFT JOIN actors a ON p.actor_did = a.did
        LEFT JOIN images i ON p.actor_did = i.post_actor_did
        AND p.rkey = i.post_rkey
    WHERE p.actor_did = sqlc.arg('actor_did')
        AND p.rkey = sqlc.arg('rkey')
    GROUP BY p.actor_did,
        p.rkey,
        a.handle,
        a.pro_pic_cid
),
Replies AS (
    SELECT p.*,
        a.handle,
        a.pro_pic_cid,
        array_agg(COALESCE(i.cid, ''))::TEXT [] as image_cids,
        array_agg(COALESCE(i.alt_text, ''))::TEXT [] as image_alts
    FROM posts p
        LEFT JOIN actors a ON p.actor_did = a.did
        LEFT JOIN images i ON p.actor_did = i.post_actor_did
        AND p.rkey = i.post_rkey
    WHERE p.parent_post_actor_did = (
            SELECT actor_did
            FROM RootPost
        )
        AND p.parent_post_rkey = (
            SELECT rkey
            FROM RootPost
        )
    GROUP BY p.actor_did,
        p.rkey,
        a.handle,
        a.pro_pic_cid
),
RootLikeCount AS (
    SELECT lc.subject_id,
        lc.num_likes
    FROM subjects s
        JOIN like_counts lc ON s.id = lc.subject_id
    WHERE s.actor_did = (
            SELECT actor_did
            FROM RootPost
        )
        AND s.rkey = (
            SELECT rkey
            FROM RootPost
        )
),
ReplyLikeCounts AS (
    SELECT s.actor_did,
        s.rkey,
        lc.num_likes
    FROM subjects s
        JOIN like_counts lc ON s.id = lc.subject_id
    WHERE s.actor_did IN (
            SELECT actor_did
            FROM Replies
        )
        AND s.rkey IN (
            SELECT rkey
            FROM Replies
        )
)
SELECT rp.*,
    rlc.num_likes AS like_count
FROM RootPost rp
    LEFT JOIN RootLikeCount rlc ON rlc.subject_id = (
        SELECT id
        FROM subjects
        WHERE actor_did = rp.actor_did
            AND rkey = rp.rkey
    )
UNION ALL
SELECT r.*,
    rlc.num_likes AS like_count
FROM Replies r
    LEFT JOIN ReplyLikeCounts rlc ON r.actor_did = rlc.actor_did
    AND r.rkey = rlc.rkey;
