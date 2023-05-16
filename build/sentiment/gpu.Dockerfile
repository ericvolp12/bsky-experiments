# Stage 1: Base layer with python and necessary libraries
FROM python:3.10-slim-buster as base

WORKDIR /app

COPY python/sentiment/poetry.lock python/sentiment/pyproject.toml /app/

RUN pip install poetry \
    && poetry config virtualenvs.create false \
    && poetry install --no-dev

RUN pip install torch==2.0.0 torchvision==0.15.1 torchaudio==2.0.1

# Stage 2: Downloading the sentiment model
FROM alpine/git:latest as model-downloader

WORKDIR /model

RUN git lfs install

RUN git clone https://hf.co/cardiffnlp/twitter-roberta-base-sentiment-latest

# Stage 3: Building the final image
FROM base as final

RUN mkdir -p /models

ENV MODEL_FROM_DISK=True

COPY --from=model-downloader /model/twitter-roberta-base-sentiment-latest /app/cardiffnlp/twitter-roberta-base-sentiment-latest
COPY python/sentiment/bsky-sentiment /app/bsky-sentiment

ENV PYTHONPATH=/app

CMD ["uvicorn", "bsky-sentiment.app:app", "--host", "0.0.0.0", "--port", "8088"]
