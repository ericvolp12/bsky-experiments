import os
import time
from opentelemetry import trace
from transformers import AutoModelForSequenceClassification, AutoTokenizer, AutoConfig
import numpy as np
from scipy.special import softmax


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
    tracer = trace.get_tracer("bsky-sentiment")
    with tracer.start_as_current_span("get_sentiment") as span:
        start = time.time()
        text = preprocess(post_text)
        encoded_input = tokenizer(text, return_tensors="pt")
        with tracer.start_as_current_span("model_inference"):
            output = model(**encoded_input)
        scores = output[0][0].detach().numpy()
        scores = softmax(scores)

        ranking = np.argsort(scores)
        ranking = ranking[::-1]
        sentiment = config.id2label[ranking[0]]
        confidence_score = scores[ranking[0]]
        processing_time = time.time() - start
        # If it takes longer than 1 second to process, add some attributes to the span
        if processing_time > 1:
            span.set_attribute("slow_process", True)
            span.set_attribute("processing_time", processing_time)
            span.set_attribute("post_text", post_text)

        return sentiment, confidence_score
