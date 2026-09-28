"""Structured JSON logging emits parseable lines with extras."""

from __future__ import annotations

import io
import json
import logging

from app.logging_config import JsonFormatter, setup_logging


def test_json_log_line_parses_and_includes_extra_field() -> None:
    setup_logging(level="INFO")
    logger = logging.getLogger("test_logging")

    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(JsonFormatter())
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    logger.info(
        "pipeline_step",
        extra={
            "document_id": "doc-123",
            "user_id": "user-1",
            "version": 2,
            "stage": "processing",
        },
    )

    line = stream.getvalue().strip()
    payload = json.loads(line)

    assert payload["message"] == "pipeline_step"
    assert payload["document_id"] == "doc-123"
    assert payload["user_id"] == "user-1"
    assert payload["version"] == 2
    assert payload["stage"] == "processing"
    assert "timestamp" in payload
    assert payload["level"] == "INFO"
