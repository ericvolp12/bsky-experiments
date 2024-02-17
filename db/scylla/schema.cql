CREATE TABLE actors (did text PRIMARY KEY, handle text) WITH default_time_to_live = 0
AND compaction = { 'class': 'SizeTieredCompactionStrategy' };

CREATE TABLE posts (
    actor_did text,
    rkey text,
    content text,
    parent_post_uri text,
    parent_relationship text,
    root_post_uri text,
    has_embedded_media tinyint,
    created_at timestamp,
    inserted_at timestamp,
    PRIMARY KEY ((actor_did, rkey), created_at)
) WITH default_time_to_live = 0
AND compaction = { 'class': 'SizeTieredCompactionStrategy' };

CREATE table images (
    cid text,
    post_uri text,
    alt_text text,
    mime_type text,
    inserted_at timestamp,
    PRIMARY KEY ((post_uri, cid))
) WITH default_time_to_live = 0
AND compaction = { 'class': 'SizeTieredCompactionStrategy' };

CREATE TABLE likes (
    actor_did text,
    rkey text,
    subject_uri text,
    created_at timestamp,
    inserted_at timestamp,
    PRIMARY KEY ((actor_did, rkey), subject_uri, created_at)
) WITH default_time_to_live = 0
AND compaction = { 'class': 'SizeTieredCompactionStrategy' };

CREATE TABLE like_counts (
    subject_uri text,
    count counter,
    PRIMARY KEY (subject_uri)
) WITH default_time_to_live = 0

CREATE TABLE labels (
    alias text,
    display_name text,
    description text,
    inserted_at timestamp,
    PRIMARY KEY ( alias, inserted_at )
) WITH default_time_to_live = 0
AND compaction = { 'class': 'SizeTieredCompactionStrategy' };

CREATE TABLE label_assignments (
    uri text,
    label text,
    inserted_at timestamp,
    PRIMARY KEY ((uri, label), inserted_at)
) WITH default_time_to_live = 0
AND compaction = { 'class': 'SizeTieredCompactionStrategy' };

CREATE TABLE actor_blocks (
    actor_did text,
    rkey text,
    target_did text,
    created_at timestamp,
    inserted_at timestamp,
    PRIMARY KEY ((actor_did, rkey), target_did, created_at)
) WITH default_time_to_live = 0
AND compaction = { 'class': 'SizeTieredCompactionStrategy' };

CREATE TABLE follows (
    actor_did text,
    rkey text,
    target_did text,
    created_at timestamp,
    inserted_at timestamp,
    PRIMARY KEY ( (actor_did, rkey), target_did, created_at )
) WITH default_time_to_live = 0
AND compaction = { 'class': 'SizeTieredCompactionStrategy' };
