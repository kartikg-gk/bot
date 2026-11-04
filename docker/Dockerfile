# A bot's computer: shell tools only. The agent loop and model keys stay in the gateway.
#   docker build -f docker/Dockerfile -t delta-bot:local docker
FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends git ripgrep \
    && rm -rf /var/lib/apt/lists/*
RUN useradd --create-home bot
USER bot
WORKDIR /workspace
