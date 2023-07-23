CREATE INDEX post_labels_gin ON post_hotness USING gin(post_labels);
CREATE INDEX author_labels_gin ON post_hotness USING gin(author_labels);
CREATE INDEX post_id ON post_hotness (id);
CREATE INDEX post_hotness_idx ON post_hotness (hotness);
