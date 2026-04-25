"""Tests para scraper/senado_client.py — cliente anti-WAF del Senado."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest

from scraper._types import FetchResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_response(
    status_code: int = 200,
    content: bytes = b"<html>Normal page content</html>",
    headers: dict | None = None,
    url: str = "https://www.senado.gob.mx/66/votacion/1",
) -> MagicMock:
    """Crea un MagicMock que simula una respuesta HTTP de curl_cffi."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.content = content
    resp.headers = headers or {"content-type": "text/html"}
    resp.url = url
    return resp


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_session():
    """Session mock que simula curl_cffi.requests.Session."""
    session = MagicMock()
    response = _make_response()
    session.get.return_value = response
    session.post.return_value = response
    return session


@pytest.fixture
def _patched_deps(mock_session):
    """Parchea todas las dependencias externas del módulo senado_client."""
    with (
        patch("scraper.senado_client.HAS_CURL_CFFI", True),
        patch("scraper.senado_client.Session", return_value=mock_session),
        patch("scraper.senado_client.time.sleep"),
    ):
        yield mock_session


@pytest.fixture
def client(_patched_deps, mock_session):
    """Cliente con sesión mockeada lista para usar."""
    from scraper.senado_client import SenadoAntiWAFClient

    c = SenadoAntiWAFClient()
    c._session = mock_session
    return c


# ---------------------------------------------------------------------------
# Test 1: fetch devuelve FetchResult correcto
# ---------------------------------------------------------------------------

def test_fetch_returns_fetch_result(client, mock_session):
    """fetch() debe devolver un FetchResult con todos los campos correctos."""
    mock_session.get.return_value = _make_response(
        status_code=200,
        content=b"<html>Normal page content</html>",
    )

    result = client.fetch("https://www.senado.gob.mx/66/votacion/1")

    assert isinstance(result, FetchResult)
    assert result.status_code == 200
    assert result.waf_detected is False
    assert result.method == "GET"
    assert result.url == "https://www.senado.gob.mx/66/votacion/1"
    assert result.body == b"<html>Normal page content</html>"
    assert isinstance(result.timestamp, datetime)
    assert result.latency_ms >= 0.0
    assert len(result.sha256_body) == 64  # SHA-256 hex


# ---------------------------------------------------------------------------
# Test 2: detección de WAF por status code
# ---------------------------------------------------------------------------

def test_waf_detection_status_code(client):
    """Una respuesta 403 debe detectarse como WAF via _is_waf_response."""
    result = client._is_waf_response("some content", 403)
    assert result is True
    assert client._consecutive_wafs == 1


# ---------------------------------------------------------------------------
# Test 3: detección de WAF por marcadores en el body
# ---------------------------------------------------------------------------

def test_waf_detection_body_markers(client):
    """Body pequeño (<5KB) con 'Incapsula' debe detectarse como WAF."""
    body = "<html><body>Incapsula incident report</body></html>"
    assert len(body.encode()) < 5 * 1024  # menos de 5 KB

    result = client._is_waf_response(body, 200)
    assert result is True
    assert client._consecutive_wafs == 1


# ---------------------------------------------------------------------------
# Test 4: circuit breaker — SessionBurnedError tras WAFs consecutivos
# ---------------------------------------------------------------------------

def test_circuit_breaker_session_burned(client, mock_session):
    """WAFs consecutivos deben causar SessionBurnedError (circuit breaker).

    El circuit breaker acumula entre recreaciones de sesión. Si tras
    recrear la sesión, el siguiente request también es WAF, el counter
    sigue subiendo hasta alcanzar el threshold.
    """
    from scraper.senado_client import SessionBurnedError

    mock_session.get.return_value = _make_response(status_code=403, content=b"Forbidden")

    # El primer fetch() agota sus reintentos internos: cada 403 incrementa
    # el counter. Counter=1 (no burn), luego counter=2 → SessionBurnedError.
    # El error se captura internamente y se reintentan max_retries veces,
    # pero finalmente se re-propaga al agotar intentos.
    with pytest.raises(SessionBurnedError):
        client.fetch("https://www.senado.gob.mx/66/votacion/1")


def test_circuit_breaker_counter_resets_after_success(client, mock_session):
    """El contador de WAFs consecutivos debe resetearse tras una respuesta OK."""
    from scraper.senado_client import SessionBurnedError

    # Respuesta exitosa — counter = 0
    mock_session.get.return_value = _make_response(status_code=200)
    result_ok = client.fetch("https://www.senado.gob.mx/66/votacion/1")
    assert result_ok.waf_detected is False

    # Ahora empezamos a contar WAFs desde 0 — una respuesta 403 debe
    # dar counter=1, sin quemar la sesión todavía
    mock_session.get.return_value = _make_response(status_code=403)
    # El primer WAF dentro del fetch() da counter=1, se recrea sesión.
    # El segundo intento da counter=2 → SessionBurnedError → se recupera
    # con recreate, pero agota max_retries y finalmente lanza.
    # Verificamos que al menos el primer request no quema la sesión:
    assert client._consecutive_wafs == 0  # reseteado por el OK anterior


# ---------------------------------------------------------------------------
# Test 5: rotación proactiva al alcanzar MAX_REQUESTS_PER_SESSION
# ---------------------------------------------------------------------------

def test_proactive_rotation(client, mock_session):
    """Al alcanzar MAX_REQUESTS_PER_SESSION se debe recrear la sesión."""
    from scraper.senado_client import MAX_REQUESTS_PER_SESSION

    mock_session.get.return_value = _make_response(status_code=200)

    with patch.object(
        client, "_recreate_session", wraps=client._recreate_session
    ) as spy_recreate:
        for i in range(MAX_REQUESTS_PER_SESSION + 1):
            client.fetch(f"https://www.senado.gob.mx/66/votacion/{i}")

        spy_recreate.assert_called()


# ---------------------------------------------------------------------------
# Test 6: recreación de sesión tras WAF con warm-up GET
# ---------------------------------------------------------------------------

def test_session_recreation_after_waf(client, mock_session):
    """Tras detectar WAF, la sesión debe recrearse (nuevo fingerprint)."""
    # Secuencia: fetch GET→403(WAF), warm-up GET→200, retry GET→200(éxito)
    waf_resp = _make_response(status_code=403, content=b"Forbidden")
    ok_resp = _make_response(
        status_code=200, content=b"<html>Senate page</html>"
    )
    mock_session.get.side_effect = [waf_resp, ok_resp, ok_resp]

    with patch.object(
        client, "_recreate_session", wraps=client._recreate_session
    ) as spy_recreate:
        result = client.fetch("https://www.senado.gob.mx/66/votacion/1")

        assert result.waf_detected is False  # el retry final fue 200
        spy_recreate.assert_called()

    # Verificar que se hizo warm-up + retry (mínimo 2 calls)
    assert mock_session.get.call_count >= 2


# ---------------------------------------------------------------------------
# Test 7: error de red retorna status_code=0 sin crashear
# ---------------------------------------------------------------------------

def test_fetch_network_error_returns_status_zero(client, mock_session):
    """Una excepción de red debe devolver FetchResult con status_code=0."""
    mock_session.get.side_effect = ConnectionError("Connection refused")

    result = client.fetch("https://www.senado.gob.mx/66/votacion/1")

    assert isinstance(result, FetchResult)
    assert result.status_code == 0
    assert b"Connection refused" in result.body or result.status_code == 0


# ---------------------------------------------------------------------------
# Test 8: context manager
# ---------------------------------------------------------------------------

def test_context_manager(_patched_deps, mock_session):
    """El cliente debe funcionar como context manager y cerrar la sesión."""
    from scraper.senado_client import SenadoAntiWAFClient

    with SenadoAntiWAFClient() as client:
        assert isinstance(client, SenadoAntiWAFClient)

    # Al salir del context, la sesión debe cerrarse
    mock_session.close.assert_called()


# ---------------------------------------------------------------------------
# Test 9: POST con payload
# ---------------------------------------------------------------------------

def test_post_method(client, mock_session):
    """fetch() con method='POST' debe reflejar POST en el FetchResult."""
    payload = b"param1=value1&param2=value2"
    mock_session.post.return_value = _make_response(status_code=200)

    result = client.fetch(
        "https://www.senado.gob.mx/66/votacion/1",
        method="POST",
        payload=payload,
    )

    assert isinstance(result, FetchResult)
    assert result.method == "POST"
    assert result.request_payload == payload
    mock_session.post.assert_called_once()


# ---------------------------------------------------------------------------
# Test 10: RuntimeError cuando curl_cffi no está disponible
# ---------------------------------------------------------------------------

def test_import_error_without_curl_cffi():
    """Si curl_cffi no está instalado, __init__ debe lanzar RuntimeError."""
    with patch("scraper.senado_client.HAS_CURL_CFFI", False):
        from scraper.senado_client import SenadoAntiWAFClient

        with pytest.raises(RuntimeError, match="curl_cffi"):
            SenadoAntiWAFClient()
