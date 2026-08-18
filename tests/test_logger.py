import json
import logging

from producer.logger import get_logger


def test_logger_emits_valid_json(capsys):
    logger = get_logger("test.logger")
    logger.setLevel(logging.INFO)
    logger.info("hello world", extra={"event": "test_event"})

    captured = capsys.readouterr()
    line = captured.out.strip().splitlines()[-1]
    payload = json.loads(line)

    assert payload["message"] == "hello world"
    assert payload["level"] == "INFO"
    assert payload["event"] == "test_event"
    assert payload["name"] == "test.logger"


def test_get_logger_does_not_duplicate_handlers():
    logger_a = get_logger("test.dup")
    logger_b = get_logger("test.dup")
    assert logger_a is logger_b
    assert len(logger_a.handlers) == 1
