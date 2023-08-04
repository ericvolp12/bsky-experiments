-- Backfill Status
-- CREATE TABLE repo_backfill_status (
--     repo TEXT NOT NULL,
--     last_backfill TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
--     seq_started BIGINT DEFAULT 0 NOT NULL,
--     state TEXT DEFAULT 'in_progress'::text NOT NULL,
--     PRIMARY KEY (repo)
-- );
-- name: CreateRepoBackfillRecord :exec
INSERT INTO repo_backfill_status (
        repo,
        last_backfill,
        seq_started,
        state
    )
VALUES ($1, $2, $3, $4);
-- name: GetRepoBackfillRecord :one
SELECT *
FROM repo_backfill_status
WHERE repo = $1;
-- name: UpdateRepoBackfillRecord :exec
UPDATE repo_backfill_status
SET last_backfill = $2,
    seq_started = $3,
    state = $4
WHERE repo = $1;
-- name: GetRepoBackfillRecords :many
SELECT *
FROM repo_backfill_status
ORDER BY last_backfill ASC
LIMIT $1 OFFSET $2;
