CREATE MATERIALIZED VIEW author_stats AS WITH postcounts AS (
    SELECT author_did,
        COUNT(id) AS num_posts
    FROM posts
    GROUP BY author_did
),
percentiles AS (
    SELECT (
            percentile_cont(0.25) WITHIN GROUP (
                ORDER BY num_posts
            ) * 1
        )::bigint AS p25,
        (
            percentile_cont(0.50) WITHIN GROUP (
                ORDER BY num_posts
            ) * 1
        )::bigint AS p50,
        (
            percentile_cont(0.75) WITHIN GROUP (
                ORDER BY num_posts
            ) * 1
        )::bigint AS p75,
        (
            percentile_cont(0.90) WITHIN GROUP (
                ORDER BY num_posts
            ) * 1
        )::bigint AS p90,
        (
            percentile_cont(0.95) WITHIN GROUP (
                ORDER BY num_posts
            ) * 1
        )::bigint AS p95,
        (
            percentile_cont(0.99) WITHIN GROUP (
                ORDER BY num_posts
            ) * 1
        )::bigint AS p99,
        (
            percentile_cont(0.995) WITHIN GROUP (
                ORDER BY num_posts
            ) * 1
        )::bigint AS p99_5,
        (
            percentile_cont(0.997) WITHIN GROUP (
                ORDER BY num_posts
            ) * 1
        )::bigint AS p99_7,
        (
            percentile_cont(0.999) WITHIN GROUP (
                ORDER BY num_posts
            ) * 1
        )::bigint AS p99_9,
        (
            percentile_cont(0.9999) WITHIN GROUP (
                ORDER BY num_posts
            ) * 1
        )::bigint AS p99_99
    FROM postcounts
),
counts AS (
    SELECT count(num_posts) AS total_authors,
        SUM(
            CASE
                WHEN num_posts > 1 THEN 1
                ELSE 0
            END
        ) AS gt_1,
        SUM(
            CASE
                WHEN num_posts > 5 THEN 1
                ELSE 0
            END
        ) AS gt_5,
        SUM(
            CASE
                WHEN num_posts > 10 THEN 1
                ELSE 0
            END
        ) AS gt_10,
        SUM(
            CASE
                WHEN num_posts > 20 THEN 1
                ELSE 0
            END
        ) AS gt_20,
        SUM(
            CASE
                WHEN num_posts > 100 THEN 1
                ELSE 0
            END
        ) AS gt_100,
        SUM(
            CASE
                WHEN num_posts > 1000 THEN 1
                ELSE 0
            END
        ) as gt_1000
    FROM postcounts
)
SELECT total_authors,
    (
        SELECT SUM(num_posts)
        FROM postcounts
    )::bigint AS total_posts,
    (
        SELECT AVG(num_posts)
        FROM postcounts
    )::float AS mean_posts_per_author,
    gt_1,
    gt_5,
    gt_10,
    gt_20,
    gt_100,
    gt_1000,
    p25,
    p50,
    p75,
    p90,
    p95,
    p99,
    p99_5,
    p99_7,
    p99_9,
    p99_99
FROM counts,
    percentiles
LIMIT 1;
CREATE MATERIALIZED VIEW top_posters AS
SELECT COUNT(p.id) AS post_count,
    a.handle,
    a.did AS author_did
FROM posts p
    JOIN authors a ON p.author_did = a.did
WHERE a.did NOT IN (
        'did:plc:jlqiqmhalnu5af3pf56jryei',
        -- Goose.art's Bot  - intern.goose.art
        'did:plc:vuwg6b5ashezjhh6lpnssljm',
        -- Spammy Bot       - xnu.kr
        'did:plc:y5smfgzb3oitolqlln3atanl',
        -- Retroid Bot      - who-up.bsky.social
        'did:plc:czze3j5772nu6gxdhben5i34',
        -- Berduck          - berduck.deepfates.com
        'did:plc:4hqjfn7m6n5hno3doamuhgef',
        -- Yui              - yui.syui.ai
        'did:plc:kwmcvt4maab47n7dgvepg4tr',
        -- Timestamp Bot    - tick.bsky.social
        'did:plc:6smdztjrq7bjjlojkrnpcnxm',
        -- Now Playinb Got  - worbler.bsky.social
        'did:plc:3tyx5envm7fms2jxgvq4pz6e',
        -- Deleted Acc      - mavs.bsky.social
        'did:plc:zznz5dqjamwecp4yogdjugx2',
        -- Deleted Acc      - shahbazi.bsky.social
        'did:plc:czcwobs37px7otals6umpd5j',
        -- News Bot         - almir.bsky.social
        'did:plc:zqnx5g75q5ygxxxmoqfdcpsc' -- Deleted Acc
    )
GROUP BY a.did,
    a.handle
ORDER BY post_count DESC
LIMIT 100;
