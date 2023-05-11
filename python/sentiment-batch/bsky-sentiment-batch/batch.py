import logging
import os
import time

import numpy
import numpy as np
import psycopg2
import torch
from psycopg2.extensions import AsIs, register_adapter
from psycopg2.extras import execute_values
from scipy.special import softmax
from transformers import AutoConfig, AutoModelForSequenceClassification, AutoTokenizer


def adapt_numpy_float32(numpy_float32):
    return AsIs(numpy_float32)


def adapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)


register_adapter(numpy.float32, adapt_numpy_float32)
register_adapter(numpy.float64, adapt_numpy_float64)

DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT")

# Database connection setup
conn = psycopg2.connect(
    dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
)

conn.set_client_encoding("UTF8")

cursor = conn.cursor()

update_cursor = conn.cursor()

logger = logging.getLogger(__name__)
logger.setLevel("INFO")
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)


def preprocess(text):
    new_text = []
    for t in text.split(" "):
        t = "@user" if t.startswith("@") and len(t) > 1 else t
        t = "http" if t.startswith("http") else t
        new_text.append(t)
    return " ".join(new_text)


MODEL = f"cardiffnlp/twitter-roberta-base-sentiment-latest"

device = "cuda:0" if torch.cuda.is_available() else "cpu"
tokenizer = AutoTokenizer.from_pretrained(MODEL)
config = AutoConfig.from_pretrained(MODEL)

model = AutoModelForSequenceClassification.from_pretrained(MODEL).to(device)
# model.save_pretrained(os.getcwd() + "/cardiffnlp/twitter-roberta-base-sentiment-latest")


def get_sentiment(post_text):
    text = preprocess(post_text)
    encoded_input = tokenizer(text, return_tensors="pt").to(device)
    output = model(**encoded_input)
    scores = output[0][0].cpu().detach().numpy()
    scores = softmax(scores)

    ranking = np.argsort(scores)
    ranking = ranking[::-1]
    sentiment = config.id2label[ranking[0]]
    confidence_score = scores[ranking[0]]
    return sentiment, confidence_score


def batch_update_posts():
    rows_updated = 0
    rows_fetched = 0
    update_stmt = """
                UPDATE posts
                SET sentiment = v1, sentiment_confidence = v2
                from (values %s) f(id, v1, v2)
                WHERE posts.id = f.id
            """

    while True:
        batch_start = time.time()
        cursor.execute(
            """
            SELECT id, text
            FROM posts
            WHERE sentiment IS NULL AND sentiment_confidence IS NULL
            ORDER BY created_at ASC
            LIMIT 250
            """
        )
        rows = cursor.fetchall()

        if not rows:
            break

        fetch_done = time.time()
        rows_fetched += len(rows)

        updates = []

        for row in rows:
            post_id, post_text = row
            if post_text != "" and len(post_text) < 250:
                sentiment, confidence_score = get_sentiment(post_text)
                sentiment_as_char = ""
                if sentiment == "positive":
                    sentiment_as_char = "p"
                elif sentiment == "negative":
                    sentiment_as_char = "n"
                else:
                    sentiment_as_char = "u"
                logging.info(
                    f"Post ID: {post_id}: {sentiment_as_char} - {confidence_score}"
                )
                updates.append(
                    (
                        post_id,
                        sentiment_as_char,
                        confidence_score,
                    )
                )
        inference_done = time.time()

        execute_values(
            update_cursor,
            update_stmt,
            updates,
            template=None,
            page_size=250,
            fetch=False,
        )
        update_done = time.time()
        rows_updated += len(rows)
        logger.info(
            f"Batch of {len(rows)} posts fetched in {fetch_done - batch_start:.2f}s, "
            f"inference done in {inference_done - fetch_done:.2f}s, "
            f"update ({update_cursor.rowcount}) done in {update_done - inference_done:.2f}s"
        )


if __name__ == "__main__":
    batch_update_posts()
