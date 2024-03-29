-- Repo Cleanup Jobs
CREATE TABLE repo_cleanup_jobs (
    job_id TEXT PRIMARY KEY,
    repo TEXT NOT NULL,
    refresh_token TEXT NOT NULL,
    cleanup_types TEXT [] NOT NULL,
    delete_older_than TIMESTAMP WITH TIME ZONE NOT NULL,
    num_deleted INTEGER NOT NULL,
    num_deleted_today INTEGER NOT NULL,
    est_num_remaining INTEGER NOT NULL,
    job_state TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    last_deleted_at TIMESTAMP WITH TIME ZONE
);
CREATE INDEX repo_cleanup_jobs_repo_idx ON repo_cleanup_jobs (repo);
CREATE INDEX repo_cleanup_jobs_job_state_idx ON repo_cleanup_jobs (job_state);
CREATE INDEX repo_cleanup_jobs_updated_at_idx ON repo_cleanup_jobs (updated_at);
