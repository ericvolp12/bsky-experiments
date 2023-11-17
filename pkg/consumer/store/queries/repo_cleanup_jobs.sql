-- -- Repo Cleanup Jobs
-- CREATE TABLE repo_cleanup_jobs (
--     job_id TEXT PRIMARY KEY,
--     repo TEXT NOT NULL,
--     refresh_token TEXT NOT NULL,
--     cleanup_types TEXT [] NOT NULL,
--     delete_older_than TIMESTAMP WITH TIME ZONE NOT NULL,
--     num_deleted INTEGER NOT NULL,
--     num_deleted_today INTEGER NOT NULL,
--     est_num_remaining INTEGER NOT NULL,
--     job_state TEXT NOT NULL,
--     created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
--     updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
--     last_deleted_at TIMESTAMP WITH TIME ZONE
-- );
-- name: UpsertRepoCleanupJob :one
INSERT INTO repo_cleanup_jobs (
        job_id,
        repo,
        refresh_token,
        cleanup_types,
        delete_older_than,
        num_deleted,
        num_deleted_today,
        est_num_remaining,
        job_state,
        updated_at,
        last_deleted_at
    )
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) ON CONFLICT (job_id) DO
UPDATE
SET repo = $2,
    refresh_token = $3,
    cleanup_types = $4,
    delete_older_than = $5,
    num_deleted = $6,
    num_deleted_today = $7,
    est_num_remaining = $8,
    job_state = $9,
    updated_at = $10,
    last_deleted_at = $11
RETURNING *;
-- name: GetRepoCleanupJob :one
SELECT *
FROM repo_cleanup_jobs
WHERE job_id = $1;
-- name: GetCleanupJobsByRepo :many
SELECT *
FROM repo_cleanup_jobs
WHERE repo = $1
ORDER BY updated_at DESC
LIMIT $2;
-- name: GetRunningCleanupJobsByRepo :many
SELECT *
FROM repo_cleanup_jobs
WHERE repo = $1
    AND job_state = 'running'
ORDER BY updated_at DESC;
-- name: DeleteRepoCleanupJob :exec
DELETE FROM repo_cleanup_jobs
WHERE job_id = $1;
-- name: GetRunningCleanupJobs :many
SELECT *
FROM repo_cleanup_jobs
WHERE job_state = 'running'
ORDER BY updated_at ASC
LIMIT $1;
