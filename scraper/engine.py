"""Motor HTTP compartido para el scraper productivo."""

from __future__ import annotations

import hashlib
import json
import logging
import random
import time
import tomllib
from datetime import datetime, timezone
from pathlib import Path

import httpx

from scraper._types import FetchResult
from f1.parsers.xp_utils import _detect_waf

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent


class HTTPScraperEngine:
    """Cliente HTTP reutilizable con rotación de UA, delays y detección de WAF."""

    def __init__(
        self,
        config: dict | None = None,
        config_path: Path | None = None,
    ) -> None:
        if config is None:
            if config_path is None:
                config_path = PROJECT_ROOT / "f1" / "config" / "xp_config.toml"
            with open(config_path, "rb") as fh:
                config = tomllib.load(fh)
        self.config = config

        timeout = self.config.get("timeout_seconds", {}).get("download", 30.0)
        self.client = httpx.Client(
            timeout=timeout,
            follow_redirects=True,
        )

        self.user_agents: list[str] = self.config.get("user_agent_pool", [])
        if not self.user_agents:
            logger.warning("user_agent_pool vacío; usando UA por defecto")
            self.user_agents = [
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            ]

        self.delay_config = self.config.get("delay", {})
        self.backoff_config = self.config.get("backoff", {})

    def __enter__(self) -> HTTPScraperEngine:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.client.close()

    # ------------------------------------------------------------------
    # Delay helpers
    # ------------------------------------------------------------------

    def _get_delay_for_source(self, source_tag: str) -> dict:
        """Devuelve la config de delay apropiada para *source_tag*."""
        if source_tag.startswith("dip"):
            return self.delay_config.get("diputados", {"base_ms": 500, "jitter_ms": 300})
        if source_tag.startswith("senado"):
            return self.delay_config.get("senado", {"base_ms": 800, "jitter_ms": 400})
        return self.delay_config.get("diputados", {"base_ms": 500, "jitter_ms": 300})

    def _apply_delay(self, source_tag: str, attempt_num: int = 1) -> None:
        """Duerme el delay base + jitter (+ backoff si attempt > 1)."""
        cfg = self._get_delay_for_source(source_tag)
        base_ms = cfg.get("base_ms", 0)
        jitter_ms = cfg.get("jitter_ms", 0)
        jitter = random.uniform(0, jitter_ms)

        backoff_ms = 0.0
        if attempt_num > 1:
            backoff_base = self.backoff_config.get("base_ms", 1000)
            multiplier = self.backoff_config.get("multiplier", 2.0)
            max_ms = self.backoff_config.get("max_ms", 16000)
            backoff_ms = min(backoff_base * (multiplier ** (attempt_num - 1)), max_ms)

        total_ms = base_ms + jitter + backoff_ms
        logger.debug(
            "Delay %s attempt=%s: base=%.0f jitter=%.0f backoff=%.0f total=%.0f ms",
            source_tag,
            attempt_num,
            base_ms,
            jitter,
            backoff_ms,
            total_ms,
        )
        time.sleep(total_ms / 1000.0)

    # ------------------------------------------------------------------
    # WAF / cache detection
    # ------------------------------------------------------------------

    def _check_waf_from_response(
        self, response: httpx.Response, source_tag: str
    ) -> tuple[bool, str | None]:
        """Detecta WAF/bloqueo mediante triggers de configuración TOML."""
        if response.status_code in (403, 502, 503, 429):
            return True, f"BLOCKING_STATUS_CODE:{response.status_code}"

        triggers = self.config.get("indeterminate_triggers", {}).get(source_tag, {})
        if not triggers:
            return False, None

        try:
            text = response.content.decode("utf-8")
        except UnicodeDecodeError:
            text = response.content.decode("iso-8859-1", errors="replace")
        text_lower = text.lower()

        for substr in triggers.get("body_substrings", []):
            if substr.lower() in text_lower:
                return True, f"BODY_SUBSTRING:{substr}"

        for hv in triggers.get("header_values", []):
            hv_lower = hv.lower()
            for key, value in response.headers.items():
                header_line = f"{key}: {value}".lower()
                if hv_lower in header_line:
                    return True, f"HEADER_VALUE:{hv}"

        return False, None

    @staticmethod
    def _detect_cache(response: httpx.Response) -> bool:
        """Devuelve True si la respuesta proviene de cache upstream."""
        cache_headers = ("x-cache", "cf-cache-status", "x-drupal-cache")
        for key in cache_headers:
            val = response.headers.get(key, "").strip().lower()
            if val.startswith("hit") or val == "hit":
                return True
        return False

    # ------------------------------------------------------------------
    # Core fetch
    # ------------------------------------------------------------------

    def _pick_ua(self) -> str:
        return random.choice(self.user_agents)

    def fetch(
        self,
        url: str,
        method: str = "GET",
        payload: bytes | None = None,
        source_tag: str = "",
        extra_headers: dict | None = None,
    ) -> FetchResult:
        """Ejecuta un request HTTP y devuelve un ``FetchResult`` estandarizado."""
        self._apply_delay(source_tag, attempt_num=1)

        headers: dict[str, str] = {"User-Agent": self._pick_ua()}
        if extra_headers:
            headers.update(extra_headers)

        payload = payload or b""
        now = datetime.now(timezone.utc)
        start = time.perf_counter()

        try:
            if method.upper() == "POST":
                response = self.client.post(
                    url, headers=headers, content=payload, follow_redirects=True
                )
            else:
                response = self.client.get(
                    url, headers=headers, follow_redirects=True
                )
            latency_ms = (time.perf_counter() - start) * 1000.0
        except httpx.RequestError as exc:
            latency_ms = (time.perf_counter() - start) * 1000.0
            error_body = str(exc).encode("utf-8")
            logger.warning("RequestError %s %s: %s", method, url, exc)
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
                request_payload=payload,
                waf_detected=False,
                cache_detected=False,
                indeterminate_reason=None,
            )

        body = response.content
        sha256_body = hashlib.sha256(body).hexdigest()
        resp_headers = dict(response.headers)
        sha256_headers = hashlib.sha256(
            json.dumps(resp_headers, sort_keys=True).encode("utf-8")
        ).hexdigest()

        # WAF detection
        waf_detected = False
        indeterminate_reason: str | None = None

        waf_result = _detect_waf(body)
        if waf_result is not None:
            waf_detected = True
            indeterminate_reason = waf_result.get("reason")

        if not waf_detected:
            waf_flag, waf_reason = self._check_waf_from_response(response, source_tag)
            if waf_flag:
                waf_detected = True
                indeterminate_reason = waf_reason

        cache_detected = self._detect_cache(response)

        logger.info(
            "Fetched %s %s → %s in %.1f ms (waf=%s cache=%s)",
            method,
            url,
            response.status_code,
            latency_ms,
            waf_detected,
            cache_detected,
        )

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
            request_payload=payload,
            waf_detected=waf_detected,
            cache_detected=cache_detected,
            indeterminate_reason=indeterminate_reason,
        )

    # ------------------------------------------------------------------
    # Retry wrapper
    # ------------------------------------------------------------------

    def fetch_with_retry(
        self,
        url: str,
        method: str = "GET",
        payload: bytes | None = None,
        source_tag: str = "",
        extra_headers: dict | None = None,
        max_retries: int = 3,
    ) -> FetchResult:
        """Llama ``fetch`` con reintentos y backoff exponencial."""
        last_result: FetchResult | None = None

        for attempt in range(1, max_retries + 1):
            # Backoff adicional entre reintentos (antes del delay base de fetch)
            if attempt > 1:
                backoff_base = self.backoff_config.get("base_ms", 1000)
                multiplier = self.backoff_config.get("multiplier", 2.0)
                max_ms = self.backoff_config.get("max_ms", 16000)
                backoff_ms = min(
                    backoff_base * (multiplier ** (attempt - 1)),
                    max_ms,
                )
                logger.debug(
                    "Retry backoff attempt=%s: sleeping %.0f ms", attempt, backoff_ms
                )
                time.sleep(backoff_ms / 1000.0)

            result = self.fetch(
                url=url,
                method=method,
                payload=payload,
                source_tag=source_tag,
                extra_headers=extra_headers,
            )
            last_result = result

            if result.waf_detected:
                logger.warning("WAF detected on attempt %s for %s", attempt, url)
                return result

            if result.status_code == 0 or result.status_code >= 500:
                logger.warning(
                    "Transient failure attempt=%s status=%s for %s",
                    attempt,
                    result.status_code,
                    url,
                )
                if attempt < max_retries:
                    continue

            # Éxito o error no reintentable
            return result

        # Se agotaron los reintentos
        assert last_result is not None
        logger.error("All %s attempts failed for %s", max_retries, url)
        return last_result
