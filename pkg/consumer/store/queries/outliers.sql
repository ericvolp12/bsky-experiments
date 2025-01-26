-- CREATE TABLE operation_outliers (
--     id BIGSERIAL PRIMARY KEY,
--     actor_did TEXT NOT NULL,
--     collection TEXT NOT NULL,
--     operation TEXT NOT NULL,
--     num_ops BIGINT NOT NULL,
--     period TEXT NOT NULL,
--     created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL
-- );
-- CREATE TABLE follower_outliers (
--     id BIGSERIAL PRIMARY KEY,
--     subject TEXT NOT NULL,
--     num_followers BIGINT NOT NULL,
--     period TEXT NOT NULL,
--     created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL
-- );
-- CREATE TABLE like_outliers (
--     id BIGSERIAL PRIMARY KEY,
--     subject TEXT NOT NULL,
--     num_likes BIGINT NOT NULL,
--     period TEXT NOT NULL,
--     created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL
-- );
-- name: InsertOperationOutliers :exec
INSERT INTO operation_outliers (
        actor_did,
        collection,
        operation,
        num_ops,
        period,
        created_at
    )
VALUES ($1, $2, $3, $4, $5, $6);
-- name: InsertFollowerOutliers :exec
INSERT INTO follower_outliers (
        subject,
        num_followers,
        period,
        created_at
    )
VALUES ($1, $2, $3, $4);
-- name: InsertLikeOutliers :exec
INSERT INTO like_outliers (
        subject,
        num_likes,
        period,
        created_at
    )
VALUES ($1, $2, $3, $4);
