# Stage 1: Base layer with python and necessary libraries
FROM python:3.10-slim-buster as base

WORKDIR /workspaces/bluesky

COPY python/object-detection/poetry.lock python/object-detection/pyproject.toml /workspaces/bluesky/

RUN pip install poetry \
    && poetry config virtualenvs.create false \
    && poetry install --no-dev

RUN pip install torch==2.0.0 torchvision==0.15.1 torchaudio==2.0.1

# Stage 2: Downloading the object-detection model
FROM alpine/git:latest as model-downloader

WORKDIR /model

RUN git lfs install

RUN git clone https://hf.co/facebook/detr-resnet-50

# Stage 3: Building the final image
FROM base as final

RUN mkdir -p /models

ENV MODEL_FROM_DISK=True

COPY --from=model-downloader /model/detr-resnet-50 /workspaces/bluesky/facebook/detr-resnet-50
COPY python/object-detection/bsky-object-detection /workspaces/bluesky/bsky-object-detection

ENV PYTHONPATH=/workspaces/bluesky

CMD ["uvicorn", "bsky-object-detection.app:app", "--host", "0.0.0.0", "--port", "8093"]
