"""Cliente HTTP anti-WAF para el Senado de la República Mexicana.

Usa ``curl_cffi`` con impersonate para evadir Incapsula WAF.
Diseñado para rotar fingerprints TLS y manejar sesiones quemadas.

Uso::

    from scraper.senado_client import SenadoAntiWAFClient

    with SenadoAntiWAFClient() as client:
        result = client.fetch("https://www.senado.gob.mx/66/votacion/123")
        print(result.status_code, result.waf_detected)
"""

from __future__ import annotations

import hashlib
import json
import logging
import random
import time
import tomllib
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scraper._types import FetchResult

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Import gracioso de curl_cffi
# ---------------------------------------------------------------------------
try:
    from curl_cffi.requests import Session

    HAS_CURL_CFFI = True
except ImportError:
    HAS_CURL_CFFI = False

# ---------------------------------------------------------------------------
# Constantes internas
# ---------------------------------------------------------------------------
MAX_REQUESTS_PER_SESSION: int = 10
WAF_CONSECUTIVE_THRESHOLD: int = 2
WAF_MAX_SIZE: int = 5 * 1024  # 5 KB — respuestas WAF son pequeñas
SENADO_BASE_URL: str = "https://www.senado.gob.mx"
SENADO_WARMUP_PATH: str = "https://www.senado.gob.mx/66/"

_IMPERSONATE_TARGETS: tuple[str, ...] = (
    "chrome",
    "safari",
    "chrome116",
    "chrome131",
    "edge",
    "chrome_android",
)

_DEFAULT_CONFIG: dict[str, Any] = {
    "delay_base_ms": 800,
    "delay_jitter_ms": 400,
    "timeout_download": 30.0,
    "max_retries": 3,
    "base_backoff": 2.0,
    "waf_body_substrings": [
        "Incapsula",
        "Access Denied",
        "403 Forbidden",
        "Attention Required",
    ],
    "waf_blocking_status_codes": [403, 502, 503, 429],
}


class SessionBurnedError(RuntimeError):
    """La sesión fue quemada por el WAF — múltiples bloqueos consecutivos."""


class SenadoAntiWAFClient:
    """Cliente HTTP anti-WAF para el Senado con ``curl_cffi`` impersonate.

    Rotación proactiva de fingerprints TLS cada ``MAX_REQUESTS_PER_SESSION``
    requests, con detección de respuestas WAF y circuit breaker.

    Ejemplo::

        with SenadoAntiWAFClient() as client:
            r = client.fetch("https://www.senado.gob.mx/66/votacion/1")
            if r.waf_detected:
                print("WAF bloqueó el request")
    """

    def __init__(self, config_path: Path | None = None) -> None:
        """Inicializar cliente con config TOML o defaults.

        Args:
            config_path: Path al TOML. Si None, busca config.toml relativo.
        """
        if not HAS_CURL_CFFI:
            raise RuntimeError(
                "curl_cffi no está instalado. "
                "Instalar con: uv pip install -e '.[senado]'"
            )

        self._config = self._load_config(config_path)
        self._delay_base: float = self._config["delay_base_ms"] / 1000.0
        self._delay_jitter: float = self._config["delay_jitter_ms"] / 1000.0
        self._timeout: float = self._config["timeout_download"]
        self._max_retries: int = self._config["max_retries"]
        self._base_backoff: float = self._config["base_backoff"]
        self._waf_body_markers: list[str] = self._config["waf_body_substrings"]
        self._blocking_codes: set[int] = set(
            self._config["waf_blocking_status_codes"]
        )

        self._fp_index: int = 0
        self._session: Session = self._create_session()
        self._request_count: int = 0
        self._consecutive_wafs: int = 0
        self._last_request_time: float = 0.0

    def __enter__(self) -> SenadoAntiWAFClient:
        """Entrar al context manager."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Salir del context manager y cerrar sesión."""
        self.close()

    def close(self) -> None:
        """Cerrar la sesión HTTP activa."""
        with suppress(Exception):
            self._session.close()

    # ------------------------------------------------------------------
    # Config
    # ------------------------------------------------------------------

    @staticmethod
    def _load_config(config_path: Path | None) -> dict[str, Any]:
        """Cargar configuración desde TOML con fallback a defaults."""
        cfg: dict[str, Any] = dict(_DEFAULT_CONFIG)

        if config_path is None:
            # Buscar config.toml relativo a este archivo
            candidates = [
                Path(__file__).resolve().parent / "config.toml",
            ]
            for candidate in candidates:
                if candidate.exists():
                    config_path = candidate
                    break

        if config_path is not None and config_path.exists():
            with open(config_path, "rb") as f:
                data = tomllib.load(f)

            # Delay
            delay_senado = data.get("delay", {}).get("senado", {})
            if "base_ms" in delay_senado:
                cfg["delay_base_ms"] = delay_senado["base_ms"]
            if "jitter_ms" in delay_senado:
                cfg["delay_jitter_ms"] = delay_senado["jitter_ms"]

            # Timeout
            timeouts = data.get("timeout_seconds", {})
            if "download" in timeouts:
                cfg["timeout_download"] = timeouts["download"]

            # WAF patterns
            waf = data.get("waf_patterns", {})
            if "body_substrings" in waf:
                cfg["waf_body_substrings"] = waf["body_substrings"]
            if "blocking_status_codes" in waf:
                cfg["waf_blocking_status_codes"] = waf["blocking_status_codes"]

        return cfg

    # ------------------------------------------------------------------
    # Session management
    # ------------------------------------------------------------------

    def _create_session(self, impersonate: str | None = None) -> Session:
        """Crear una nueva sesión ``curl_cffi`` con impersonate."""
        target = impersonate or _IMPERSONATE_TARGETS[self._fp_index]
        session = Session(impersonate=target)
        return session

    def _recreate_session(self, skip_warmup: bool = False) -> None:
        """Rotar fingerprint y recrear sesión.

        Cierra la sesión actual, rota al siguiente fingerprint del pool,
        crea nueva sesión y ejecuta un warm-up request obligatorio.
        """
        # Cerrar sesión actual
        with suppress(Exception):
            self._session.close()

        # Rotar fingerprint
        self._fp_index = (self._fp_index + 1) % len(_IMPERSONATE_TARGETS)

        # Crear nueva sesión
        self._session = self._create_session()
        self._request_count = 0
        # NO resetear _consecutive_wafs aquí — el circuit breaker debe
        # persistir entre recreaciones para detectar WAFs persistentes.
        # Solo se resetea tras una respuesta exitosa (_is_waf_response).

        # Warm-up obligatorio: GET a la raíz del Senado
        if not skip_warmup:
            with suppress(Exception):
                self._session.get(
                    SENADO_WARMUP_PATH,
                    timeout=self._timeout,
                    http_version="v1",
                )
            # Pausa post-warmup para rotación proactiva
            time.sleep(2.0)

    # ------------------------------------------------------------------
    # WAF detection
    # ------------------------------------------------------------------

    def _is_waf_response(self, content: str, status_code: int) -> bool:
        """Determinar si la respuesta es un bloqueo WAF.

        Verifica status codes de bloqueo y marcadores en el body
        (case-insensitive). Actualiza el circuit breaker interno.

        Args:
            content: Body de la respuesta como texto.
            status_code: Código de estado HTTP.

        Returns:
            True si se detectó respuesta WAF.

        Raises:
            SessionBurnedError: Si se supera el umbral de WAFs consecutivos.
        """
        is_blocking_code = status_code in self._blocking_codes

        # Status codes de bloqueo definitivo
        if is_blocking_code and status_code in (403, 503):
            self._consecutive_wafs += 1
            if self._consecutive_wafs >= WAF_CONSECUTIVE_THRESHOLD:
                raise SessionBurnedError(
                    f"Sesión quemada: {self._consecutive_wafs} bloqueos WAF consecutivos "
                    f"(último status={status_code})"
                )
            return True

        # Body markers para respuestas no-bloqueantes pero sospechosas
        if not is_blocking_code and len(content.encode("utf-8", errors="replace")) < WAF_MAX_SIZE:
            content_lower = content.lower()
            markers_lower = [m.lower() for m in self._waf_body_markers]
            # Marcadores adicionales conocidos de Incapsula
            extra_markers = ["incident_id", "waf block", "forbidden"]
            all_markers = markers_lower + extra_markers

            for marker in all_markers:
                if marker in content_lower:
                    self._consecutive_wafs += 1
                    if self._consecutive_wafs >= WAF_CONSECUTIVE_THRESHOLD:
                        raise SessionBurnedError(
                            f"Sesión quemada: {self._consecutive_wafs} bloqueos WAF consecutivos "
                            f"(marker='{marker}' en body)"
                        )
                    return True

        # Si llegamos aquí con status de bloqueo pero no 403/503 (ej. 429, 502)
        if is_blocking_code:
            self._consecutive_wafs += 1
            if self._consecutive_wafs >= WAF_CONSECUTIVE_THRESHOLD:
                raise SessionBurnedError(
                    f"Sesión quemada: {self._consecutive_wafs} bloqueos WAF consecutivos "
                    f"(status={status_code})"
                )
            return True

        # Sin WAF — resetear contador consecutivo
        self._consecutive_wafs = 0
        return False

    # ------------------------------------------------------------------
    # Rate limiting
    # ------------------------------------------------------------------

    def _rate_limit(self) -> None:
        """Aplicar delay base + jitter entre requests."""
        now = time.monotonic()
        elapsed = now - self._last_request_time
        delay = self._delay_base + random.uniform(0, self._delay_jitter)
        wait = max(0.0, delay - elapsed)
        if wait > 0:
            time.sleep(wait)
        self._last_request_time = time.monotonic()

    # ------------------------------------------------------------------
    # Fetch principal
    # ------------------------------------------------------------------

    def fetch(
        self,
        url: str,
        method: str = "GET",
        payload: bytes | None = None,
        source_tag: str = "",
        extra_headers: dict | None = None,
    ) -> FetchResult:
        """Ejecutar un request HTTP anti-WAF al Senado.

        Maneja rate limiting, rotación proactiva de sesión, detección WAF
        y reintentos con backoff exponencial.

        Args:
            url: URL objetivo.
            method: Método HTTP (GET o POST).
            payload: Body para POST requests.
            source_tag: Etiqueta de origen para tracing (no usada en request).
            extra_headers: Headers adicionales.

        Returns:
            ``FetchResult`` con todos los campos poblados.
        """
        # Rate limiting
        self._rate_limit()

        # Rotación proactiva
        if self._request_count >= MAX_REQUESTS_PER_SESSION:
            self._recreate_session()

        headers: dict[str, str] = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "es-MX,es;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
        }
        if extra_headers:
            headers.update(extra_headers)

        last_result: FetchResult | None = None

        for attempt in range(self._max_retries + 1):
            now = datetime.now(UTC)
            start = time.monotonic()

            try:
                if method.upper() == "POST":
                    response = self._session.post(
                        url,
                        timeout=self._timeout,
                        http_version="v1",
                        headers=headers,
                        data=payload or b"",
                    )
                else:
                    response = self._session.get(
                        url,
                        timeout=self._timeout,
                        http_version="v1",
                        headers=headers,
                    )

                latency_ms = (time.monotonic() - start) * 1000.0
                self._request_count += 1

                # Decodificar body
                body: bytes = response.content
                try:
                    text = body.decode("utf-8")
                except UnicodeDecodeError:
                    text = body.decode("iso-8859-1", errors="replace")

                # Verificar WAF
                waf_detected = False
                indeterminate_reason: str | None = None
                try:
                    waf_detected = self._is_waf_response(text, response.status_code)
                except SessionBurnedError:
                    # Sesión quemada — recrear y reintentar si quedan intentos
                    if attempt < self._max_retries:
                        backoff = self._base_backoff ** (attempt + 1)
                        time.sleep(backoff)
                        self._recreate_session(skip_warmup=False)
                        continue
                    raise

                if waf_detected and attempt < self._max_retries:
                    # WAF detectado — backoff exponencial y reintentar
                    backoff = self._base_backoff ** (attempt + 1)
                    time.sleep(backoff)
                    self._recreate_session(skip_warmup=False)
                    continue

                if waf_detected:
                    indeterminate_reason = "WAF_CHALLENGE"

                # Construir FetchResult
                sha256_body = hashlib.sha256(body).hexdigest()
                resp_headers = dict(response.headers.items())
                sha256_headers = hashlib.sha256(
                    json.dumps(resp_headers, sort_keys=True).encode()
                ).hexdigest()

                return FetchResult(
                    url=str(response.url),
                    method=method.upper(),
                    status_code=response.status_code,
                    body=body,
                    headers=resp_headers,
                    latency_ms=latency_ms,
                    timestamp=now,
                    sha256_body=sha256_body,
                    sha256_headers=sha256_headers,
                    request_payload=payload or b"",
                    waf_detected=waf_detected,
                    cache_detected=False,
                    indeterminate_reason=indeterminate_reason,
                )

            except SessionBurnedError:
                raise

            except Exception as exc:
                # Error de red u otro — construir FetchResult de error
                latency_ms = (time.monotonic() - start) * 1000.0
                error_body = str(exc).encode("utf-8")

                if attempt < self._max_retries:
                    backoff = self._base_backoff ** (attempt + 1)
                    time.sleep(backoff)
                    with suppress(Exception):
                        self._recreate_session(skip_warmup=True)
                    continue

                return FetchResult(
                    url=url,
                    method=method.upper(),
                    status_code=0,
                    body=error_body,
                    headers={},
                    latency_ms=latency_ms,
                    timestamp=now,
                    sha256_body=hashlib.sha256(error_body).hexdigest(),
                    sha256_headers=None,
                    request_payload=payload or b"",
                    waf_detected=False,
                    cache_detected=False,
                    indeterminate_reason=f"NETWORK_ERROR: {type(exc).__name__}",
                )

        # No debería llegar aquí, pero por seguridad
        return last_result  # type: ignore[return-value]
