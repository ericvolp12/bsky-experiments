-- name: IncrementRepostCountByN :exec
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
INSERT INTO repost_counts (subject_id, num_reposts)
SELECT id,
    $3
FROM subj ON CONFLICT (subject_id) DO
UPDATE
SET num_reposts = repost_counts.num_reposts + EXCLUDED.num_reposts;
-- name: DecrementRepostCountByN :exec
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
INSERT INTO repost_counts (subject_id, num_reposts)
SELECT id,
    - sqlc.arg('num_reposts')::int
FROM subj ON CONFLICT (subject_id) DO
UPDATE
SET num_reposts = GREATEST(
        0,
        repost_counts.num_reposts + EXCLUDED.num_reposts
    );
-- name: DeleteRepostCount :exec
DELETE FROM repost_counts
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
-- name: GetRepostCount :one
SELECT nlc.*
FROM repost_counts nlc
    JOIN subjects s ON nlc.subject_id = s.id
WHERE s.actor_did = $1
    AND s.col = (
        SELECT id
        FROM collections
        WHERE name = sqlc.arg('collection')
    )
    AND s.rkey = $2
LIMIT 1;
