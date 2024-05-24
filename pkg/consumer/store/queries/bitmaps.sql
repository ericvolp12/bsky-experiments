-- CREATE TABLE stats_bitmaps (
--     id STRING PRIMARY KEY,
--     bitmap BYTEA,
--     updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
--     created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL
-- );
-- name: UpsertBitmap :exec
INSERT INTO stats_bitmaps (
        id,
        bitmap,
        created_at,
        updated_at
    )
VALUES ($1, $2, $3, $3) ON CONFLICT (id) DO
UPDATE
SET bitmap = EXCLUDED.bitmap,
    updated_at = EXCLUDED.updated_at
WHERE stats_bitmaps.id = EXCLUDED.id;
-- name: GetBitmapByID :one
SELECT *
FROM stats_bitmaps
WHERE id = $1;
