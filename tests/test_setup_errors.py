"""A bot that cannot start says why, instead of failing without a word."""

import logging

from delta_model.settings import ConfigError

from delta_gateway.service import Service
from tests.test_gateway import Bot, config, settle, update


def missing_key(config):
    raise ConfigError("ANTHROPIC_API_KEY is not set. Get one at the provider's console.")


async def test_missing_model_key_is_reported(tmp_path, caplog):
    c, bot = config(tmp_path), Bot()
    service = Service(c, bot, factory="tests.test_setup_errors:missing_key")
    try:
        with caplog.at_level(logging.WARNING):
            await service.store.admit([update(1)], c)
            await settle(service)
        sent = [text for _, text, _ in bot.sent]
        assert any("ANTHROPIC_API_KEY is not set" in text for text in sent), sent
        assert not any("Task interrupted" in text for text in sent)
        assert "ANTHROPIC_API_KEY is not set" in caplog.text
        statuses = [row[2] for row in service.store.issues()]
        assert statuses == ["failed"]
    finally:
        await service.close()
