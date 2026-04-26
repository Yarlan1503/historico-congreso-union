"""Tipos compartidos del módulo scraper productivo.

Este módulo define los contratos de datos entre engine, pipeline, persistence
y sources. NO contiene lógica de negocio.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class FetchResult:
    """Resultado estandarizado de una operación HTTP del engine.

    Attributes:
        url: URL final solicitada (post-redirects).
        method: Método HTTP en mayúsculas (GET/POST).
        status_code: Código de estado HTTP (0 en caso de excepción de red).
        body: Cuerpo de respuesta en bytes crudos.
        headers: Headers de respuesta como dict plano.
        latency_ms: Tiempo transcurrido desde el request hasta la respuesta.
        timestamp: Momento de captura (datetime aware, UTC preferido).
        sha256_body: Hash SHA-256 del body.
        sha256_headers: Hash SHA-256 de la serialización JSON de headers, o None.
        request_payload: Payload enviado en el request (bytes).
        waf_detected: True si el engine detectó WAF/bloqueo.
        cache_detected: True si se detectó respuesta desde cache.
        indeterminate_reason: Código de razón si es INDETERMINATE (ej. WAF_CHALLENGE).
    """

    url: str
    method: str
    status_code: int
    body: bytes
    headers: dict[str, str]
    latency_ms: float
    timestamp: datetime
    sha256_body: str
    sha256_headers: str | None = None
    request_payload: bytes = b""
    waf_detected: bool = False
    cache_detected: bool = False
    indeterminate_reason: str | None = None


@dataclass
class ProcessResult:
    """Resultado del pipeline tras parsear un ``FetchResult``.

    Los campos ``source_asset``, ``vote_event``, etc. contienen dicts planos
    con los argumentos necesarios para instanciar los modelos Pydantic de
    ``f2.models``. El persistence layer se encarga de la conversión final.
    """

    fetch_result: FetchResult
    classification: str  # SUCCESS | INDETERMINATE | FAIL
    source_asset: dict[str, Any] | None = None
    vote_event: dict[str, Any] | None = None
    vote_event_asset: dict[str, Any] | None = None
    casts: list[dict[str, Any]] = field(default_factory=list)
    counts: list[dict[str, Any]] = field(default_factory=list)
    parsed_data: dict[str, Any] | None = None
    parser_errors: list[str] = field(default_factory=list)
