ALTER TABLE post_likes
SET (
        autovacuum_vacuum_scale_factor = 0,
        autovacuum_vacuum_threshold = 10000,
        autovacuum_analyze_scale_factor = 0,
        autovacuum_analyze_threshold = 10000
    );
