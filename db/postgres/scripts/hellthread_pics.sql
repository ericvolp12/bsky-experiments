CREATE OR REPLACE FUNCTION add_hellthread_pics_label() RETURNS TRIGGER AS $$ BEGIN IF NEW.root_post_id = '3juzlwllznd24'
    AND NEW.has_embedded_media = true THEN
INSERT INTO post_labels (post_id, label)
VALUES (NEW.id, 'hellthread:pics') ON CONFLICT (post_id, label) DO NOTHING;
END IF;
RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER add_hellthread_pics_label_trigger
AFTER
INSERT ON posts FOR EACH ROW EXECUTE FUNCTION add_hellthread_pics_label();
