-- name: GetPostWithAuthorHandle :one
SELECT p.id,
    p.text,
    p.parent_post_id,
    p.root_post_id,
    p.author_did,
    a.handle,
    p.created_at,
    p.has_embedded_media,
    p.parent_relationship,
    p.sentiment,
    p.sentiment_confidence,
    p.indexed_at,
    COALESCE(
        json_agg(
            json_build_object(
                'cid',
                i.cid,
                'post_id',
                i.post_id,
                'author_did',
                i.author_did,
                'alt_text',
                i.alt_text,
                'mime_type',
                i.mime_type,
                'fullsize_url',
                i.fullsize_url,
                'thumbnail_url',
                i.thumbnail_url,
                'created_at',
                i.created_at,
                'cv_completed',
                i.cv_completed,
                'cv_run_at',
                i.cv_run_at,
                'cv_classes',
                i.cv_classes
            )
        ) FILTER (
            WHERE i.cid IS NOT NULL
        ),
        '[]'
    ) as images
FROM posts p
    LEFT JOIN images i ON p.id = i.post_id
    LEFT JOIN authors a ON p.author_did = a.did
WHERE p.id = $1
GROUP BY p.id,
    a.handle;
