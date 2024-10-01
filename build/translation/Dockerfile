# Stage 1: Base layer with python and necessary libraries
FROM python:3.10-slim-buster as base

WORKDIR /app

COPY python/translation/poetry.lock python/translation/pyproject.toml /app/

RUN pip install poetry \
    && poetry config virtualenvs.create false \
    && poetry install --no-dev

RUN pip install torch==2.0.0+cpu torchvision==0.15.1+cpu torchaudio==2.0.1 --index-url https://download.pytorch.org/whl/cpu

# Stage 2: Downloading the translation model
FROM alpine/git:latest as model-downloader

WORKDIR /model

RUN git lfs install

RUN git clone https://hf.co/google-t5/t5-small

# Stage 3: Building the final image
FROM base as final

RUN mkdir -p /models

ENV MODEL_FROM_DISK=True
COPY --from=model-downloader /model/t5-small /app/google-t5/t5-small

COPY python/translation/translation /app/translation

ENV PYTHONPATH=/app

CMD ["uvicorn", "translation.app:app", "--host", "0.0.0.0", "--port", "8093"]
