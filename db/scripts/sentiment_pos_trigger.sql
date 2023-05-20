CREATE OR REPLACE FUNCTION add_positive_sentiment_label() RETURNS TRIGGER AS $$
BEGIN
    IF NEW.sentiment = 'p  ' AND NEW.sentiment_confidence > 0.65 THEN
        INSERT INTO post_labels (post_id, label)
        VALUES (NEW.id, 'sentiment:pos')
        ON CONFLICT (post_id, label) DO NOTHING;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER add_positive_sentiment_label_trigger
AFTER INSERT ON posts
FOR EACH ROW
EXECUTE FUNCTION add_positive_sentiment_label();
