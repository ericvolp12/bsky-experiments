# Stage 1: Base layer with python and necessary libraries
FROM python:3.10-slim-buster as base

WORKDIR /app

COPY python/object-detection/poetry.lock python/object-detection/pyproject.toml /app/

RUN pip install poetry \
    && poetry config virtualenvs.create false \
    && poetry install --no-dev

RUN pip install torch==2.0.0+cpu torchvision==0.15.1+cpu torchaudio==2.0.1 --index-url https://download.pytorch.org/whl/cpu

# Stage 2: Downloading the object-detection model
FROM alpine/git:latest as model-downloader

WORKDIR /model

RUN git lfs install

RUN git clone https://hf.co/openai/clip-vit-large-patch14

# Stage 3: Building the final image
FROM base as final

RUN mkdir -p /models

ENV MODEL_FROM_DISK=True

COPY --from=model-downloader /model/clip-vit-large-patch14 /app/openai/clip-vit-large-patch14
COPY python/object-detection/bsky-object-detection /app/bsky-object-detection

ENV PYTHONPATH=/app

CMD ["uvicorn", "bsky-object-detection.app:app", "--host", "0.0.0.0", "--port", "8093"]
