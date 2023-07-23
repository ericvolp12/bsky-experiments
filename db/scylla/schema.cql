CREATE TABLE actors (did text PRIMARY KEY, handle text) WITH default_time_to_live = 0
AND compaction = { 'class': 'SizeTieredCompactionStrategy' };
CREATE TABLE posts (
    uri text PRIMARY KEY,
    content text,
    parent_post_uri text,
    parent_relationship text,
    root_post_uri text,
    has_embedded_media tinyint,
    created_at timestamp,
    inserted_at timestamp
) WITH default_time_to_live = 0
AND compaction = { 'class': 'SizeTieredCompactionStrategy' };
CREATE table images (
    cid text,
    post_uri text,
    alt_text text,
    mime_type text,
    fullsize_url text,
    thumbnail_url text,
    inserted_at timestamp,
    PRIMARY KEY (cid, post_uri)
) WITH default_time_to_live = 0
AND compaction = { 'class': 'SizeTieredCompactionStrategy' };
CREATE TABLE label_assignments (
    uri text,
    label text,
    inserted_at timestamp,
    PRIMARY KEY (uri, label)
) WITH default_time_to_live = 0
AND compaction = { 'class': 'SizeTieredCompactionStrategy' };
CREATE TABLE likes (
    uri text PRIMARY KEY,
    post_uri text,
    created_at timestamp,
    inserted_at timestamp
) WITH default_time_to_live = 0
AND compaction = { 'class': 'SizeTieredCompactionStrategy' };
CREATE TABLE actor_blocks (
    uri text PRIMARY KEY,
    target_did text,
    created_at timestamp,
    inserted_at timestamp,
    PRIMARY KEY (actor_did, target_did)
) WITH default_time_to_live = 0
AND compaction = { 'class': 'SizeTieredCompactionStrategy' };
CREATE TABLE labels (
    id uuid PRIMARY KEY,
    lookup_alias text,
    display_name text,
    description text,
    inserted_at timestamp,
) WITH default_time_to_live = 0
AND compaction = { 'class': 'SizeTieredCompactionStrategy' };
CREATE TABLE follows (
    uri text PRIMARY KEY,
    target_did text,
    created_at timestamp,
    inserted_at timestamp
) WITH default_time_to_live = 0
