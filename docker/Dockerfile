# Build from a directory holding the Delta wheel:
#   docker build -f docker/Dockerfile -t delta-bot:local <wheel-directory>
FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends git ripgrep \
    && rm -rf /var/lib/apt/lists/*
COPY deltaa-*.whl /tmp/
RUN pip install --no-cache-dir /tmp/deltaa-*.whl && rm /tmp/deltaa-*.whl
RUN useradd --create-home bot
USER bot
WORKDIR /workspace
