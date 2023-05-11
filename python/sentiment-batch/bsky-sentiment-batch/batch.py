import os
import psycopg2
from psycopg2.extras import execute_values
from sqlalchemy import create_engine
from transformers import AutoModelForSequenceClassification, AutoTokenizer, AutoConfig
import numpy as np
from scipy.special import softmax


DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT")

# Database connection setup
conn = psycopg2.connect(
    dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
)
cursor = conn.cursor()

# Use SQLAlchemy engine for more efficient updates
engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)


def preprocess(text):
    new_text = []
    for t in text.split(" "):
        t = "@user" if t.startswith("@") and len(t) > 1 else t
        t = "http" if t.startswith("http") else t
        new_text.append(t)
    return " ".join(new_text)


MODEL = f"cardiffnlp/twitter-roberta-base-sentiment-latest"

tokenizer = AutoTokenizer.from_pretrained(MODEL)
config = AutoConfig.from_pretrained(MODEL)

model = AutoModelForSequenceClassification.from_pretrained(MODEL)
model.save_pretrained(os.getcwd() + "/cardiffnlp/twitter-roberta-base-sentiment-latest")


def get_sentiment(post_text):
    text = preprocess(post_text)
    encoded_input = tokenizer(text, return_tensors="pt")
    output = model(**encoded_input)
    scores = output[0][0].detach().numpy()
    scores = softmax(scores)

    ranking = np.argsort(scores)
    ranking = ranking[::-1]
    sentiment = config.id2label[ranking[0]]
    confidence_score = scores[ranking[0]]
    return sentiment, confidence_score


def batch_update_posts():
    while True:
        cursor.execute(
            """
            SELECT id, text
            FROM posts
            WHERE sentiment IS NULL AND sentiment_confidence IS NULL
            LIMIT 250
            """
        )
        rows = cursor.fetchall()

        if not rows:
            break

        updates = []
        for row in rows:
            post_id, text = row
            sentiment, confidence_score = get_sentiment(text)
            updates.append((sentiment, confidence_score, post_id))

        with engine.begin() as connection:
            connection.execute(
                """
                UPDATE posts
                SET sentiment = data.sentiment, sentiment_confidence = data.sentiment_confidence
                FROM (VALUES %s) AS data (sentiment, sentiment_confidence, id)
                WHERE posts.id = data.id
                """,
                updates,
            )


if __name__ == "__main__":
    batch_update_posts()
