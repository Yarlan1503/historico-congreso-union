"""Tests para scraper/engine.py."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from scraper._types import FetchResult
from scraper.engine import HTTPScraperEngine


@pytest.fixture
def minimal_config():
    return {
        "timeout_seconds": {"download": 30.0},
        "user_agent_pool": ["TestAgent/1.0"],
        "delay": {"diputados": {"base_ms": 0, "jitter_ms": 0}},
        "backoff": {"base_ms": 0, "multiplier": 2.0, "max_ms": 16000},
        "indeterminate_triggers": {},
    }


def test_fetch_returns_fetch_result(minimal_config):
    """fetch() debe devolver un FetchResult con los campos correctos."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.content = b"test"
    mock_response.headers = {}
    mock_response.url = "http://example.com/test"

    with (
        patch("httpx.Client") as MockClient,
        patch("scraper.engine.time.sleep", return_value=None),
        patch("scraper.engine.logger"),
    ):
        instance = MockClient.return_value
        instance.get.return_value = mock_response

        engine = HTTPScraperEngine(config=minimal_config)
        result = engine.fetch("http://example.com/test", source_tag="dip_test")

    assert isinstance(result, FetchResult)
    assert result.url == "http://example.com/test"
    assert result.method == "GET"
    assert result.status_code == 200
    assert result.body == b"test"
    assert result.headers == {}
    assert result.waf_detected is False
    assert result.cache_detected is False
    assert isinstance(result.timestamp, datetime)
    assert result.timestamp.tzinfo is not None
    assert result.latency_ms >= 0.0
    assert result.sha256_body is not None
