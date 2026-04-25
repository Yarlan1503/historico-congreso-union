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


def test_ua_consistent_within_session(minimal_config):
    """El UA debe ser el mismo para todos los requests dentro de una sesión."""
    config = minimal_config
    config["user_agent_pool"] = ["Agent-A", "Agent-B", "Agent-C"]

    with (
        patch("httpx.Client") as MockClient,
        patch("scraper.engine.time.sleep", return_value=None),
        patch("scraper.engine.logger"),
    ):
        instance = MockClient.return_value
        instance.get.return_value = MagicMock(
            status_code=200, content=b"test", headers={}, url="http://ex.com"
        )

        engine = HTTPScraperEngine(config=config)
        ua_used = engine.session_ua
        assert ua_used in ["Agent-A", "Agent-B", "Agent-C"]

        # Multiple fetches en la misma sesión
        for _ in range(5):
            engine.fetch("http://example.com/test", source_tag="test")
            call_headers = instance.get.call_args[1]["headers"]
            assert call_headers["User-Agent"] == ua_used


def test_ua_rotates_between_sessions(minimal_config):
    """El UA debe variar entre diferentes sesiones (instancias)."""
    config = minimal_config
    config["user_agent_pool"] = [f"Agent-{i}" for i in range(10)]

    uas_seen = set()
    for _ in range(50):
        with (
            patch("httpx.Client"),
            patch("scraper.engine.time.sleep", return_value=None),
            patch("scraper.engine.logger"),
        ):
            engine = HTTPScraperEngine(config=config)
            uas_seen.add(engine.session_ua)

    # Con 50 intentos y 10 UAs, deberíamos ver al menos 5 distintos
    assert len(uas_seen) >= 5


def test_complementary_headers_rotated(minimal_config):
    """Accept-Language y Accept-Encoding deben rotar entre requests."""
    with (
        patch("httpx.Client") as MockClient,
        patch("scraper.engine.time.sleep", return_value=None),
        patch("scraper.engine.logger"),
    ):
        instance = MockClient.return_value
        instance.get.return_value = MagicMock(
            status_code=200, content=b"test", headers={}, url="http://ex.com"
        )

        engine = HTTPScraperEngine(config=minimal_config)
        languages_seen = set()
        encodings_seen = set()

        for _ in range(20):
            engine.fetch("http://example.com/test", source_tag="test")
            call_headers = instance.get.call_args[1]["headers"]
            languages_seen.add(call_headers["Accept-Language"])
            encodings_seen.add(call_headers["Accept-Encoding"])

    # Deberíamos ver variedad en headers complementarios
    assert len(languages_seen) >= 2
    assert len(encodings_seen) >= 2


def test_default_pool_has_12_plus_uas():
    """El pool built-in debe tener al menos 12 UAs."""
    config = {
        "timeout_seconds": {"download": 30.0},
        "delay": {},
        "backoff": {},
    }
    with patch("httpx.Client"), patch("scraper.engine.logger"):
        engine = HTTPScraperEngine(config=config)
    assert len(engine.user_agents) >= 12
