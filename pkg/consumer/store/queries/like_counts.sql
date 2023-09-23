-- name: CreateLikeCount :exec
INSERT INTO like_counts (
        subject_id,
        num_likes,
        updated_at,
        subject_created_at
    )
VALUES ($1, $2, $3, $4);
-- name: IncrementLikeCountByN :exec
WITH subj AS (
    SELECT id
    FROM subjects
    WHERE actor_did = $1
        AND col = (
            SELECT id
            FROM collections
            WHERE name = sqlc.arg('collection')
        )
        AND rkey = $2
)
INSERT INTO like_counts (subject_id, num_likes)
SELECT id,
    $3
FROM subj ON CONFLICT (subject_id) DO
UPDATE
SET num_likes = like_counts.num_likes + EXCLUDED.num_likes;
-- name: IncrementLikeCountByNWithSubject :exec
INSERT INTO like_counts (subject_id, num_likes)
VALUES ($1, $2)
ON CONFLICT (subject_id) DO
UPDATE
SET num_likes = like_counts.num_likes + EXCLUDED.num_likes;
-- name: DecrementLikeCountByN :exec
WITH subj AS (
    SELECT id
    FROM subjects
    WHERE actor_did = $1
        AND col = (
            SELECT id
            FROM collections
            WHERE name = sqlc.arg('collection')
        )
        AND rkey = $2
)
INSERT INTO like_counts (subject_id, num_likes)
SELECT id,
    - sqlc.arg('num_likes')::int
FROM subj ON CONFLICT (subject_id) DO
UPDATE
SET num_likes = GREATEST(
        0,
        like_counts.num_likes + EXCLUDED.num_likes
    );
-- name: DeleteLikeCount :exec
DELETE FROM like_counts
WHERE subject_id IN (
        SELECT id
        FROM subjects
        WHERE actor_did = $1
            AND col = (
                SELECT id
                FROM collections
                WHERE name = sqlc.arg('collection')
            )
            AND rkey = $2
    );
-- name: GetLikeCount :one
SELECT nlc.*
FROM like_counts nlc
    JOIN subjects s ON nlc.subject_id = s.id
WHERE s.actor_did = $1
    AND s.col = (
        SELECT id
        FROM collections
        WHERE name = sqlc.arg('collection')
    )
    AND s.rkey = $2
LIMIT 1;
