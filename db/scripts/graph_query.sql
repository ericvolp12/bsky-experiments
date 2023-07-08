WITH author_relationships AS (
    SELECT p1.author_did AS source,
        a1.handle AS sourceHandle,
        p2.author_did AS target,
        a2.handle AS targetHandle,
        p2.created_at
    FROM posts p1
        INNER JOIN posts p2 ON p1.id = p2.parent_post_id
        AND p1.author_did <> p2.author_did
        INNER JOIN authors a1 ON p1.author_did = a1.did
        INNER JOIN authors a2 ON p2.author_did = a2.did
    WHERE p2.parent_relationship IN ('r', 'q')
),
relationship_weights AS (
    SELECT source,
        sourceHandle,
        target,
        targetHandle,
        EXP(
            -0.01 * EXTRACT(
                EPOCH
                FROM AGE(NOW(), created_at)
            ) / 86400
        ) AS weight
    FROM author_relationships
)
SELECT source,
    sourceHandle,
    target,
    targetHandle,
    SUM(weight) AS weight
FROM relationship_weights
GROUP BY source,
    sourceHandle,
    target,
    targetHandle;
