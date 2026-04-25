"""Helpers compartidos para parsers transductores (prefijo XP)."""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

try:
    from f1.parsers.xp_types import XPCounts, XPIndeterminate
except ImportError:
    from xp_types import XPCounts, XPIndeterminate

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Decodificación y corrección de mojibake
# ---------------------------------------------------------------------------


def _decode_body(body: bytes) -> tuple[str, str]:
    """Decodifica ``body`` intentando UTF-8 primero y fallback a ISO-8859-1.

    Devuelve una tupla ``(texto_decodificado, encoding_usado)``.
    """
    try:
        text = body.decode("utf-8")
        used = "utf-8"
    except UnicodeDecodeError:
        logger.warning("UTF-8 decoding failed, falling back to ISO-8859-1")
        text = body.decode("iso-8859-1")
        used = "iso-8859-1"

    text = _fix_mojibake(text)
    return text, used


# Secuencias típicas de mojibake: byte 0xC3 (se ve como A-tilde al decodificar
# como latin1) seguido de un byte de continuación UTF-8 (0x80-0xBF).
_MOJIBAKE_PATTERN = re.compile(r"Ã[\x80-\xbf]")


def _fix_mojibake(text: str, min_matches: int = 2) -> str:
    """Corrige mojibake del tipo UTF-8 interpretado como Latin-1.

    Detecta secuencias donde el byte 0xC3 (que se ve como A-tilde al decodificar
    como latin1) va seguido de un byte de continuación UTF-8 (0x80-0xBF).
    Si hay al menos ``min_matches`` ocurrencias, corrige solo esas secuencias
    re-encodeando latin1 -> bytes -> decode UTF-8.

    Args:
        text: Texto decodificado (posiblemente con mojibake).
        min_matches: Mínimo de ocurrencias para aplicar corrección
            (default 2 para HTML completo; usar 1 para nombres individuales).

    Returns:
        Texto corregido, o el original si no se detecta mojibake o si el
        re-decode falla.
    """
    matches = _MOJIBAKE_PATTERN.findall(text)
    if len(matches) < min_matches:
        return text

    def _replacer(match: re.Match) -> str:
        seq = match.group(0)
        try:
            return seq.encode("latin1").decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            return seq

    return _MOJIBAKE_PATTERN.sub(_replacer, text)


def _fix_mojibake_name(text: str) -> str:
    """Corrige mojibake para nombres individuales (umbral 1 ocurrencia)."""
    return _fix_mojibake(text, min_matches=1)


# ---------------------------------------------------------------------------
# Detección de WAF / bloqueo / cache / timeout
# ---------------------------------------------------------------------------

_WAF_CHALLENGE_MARKERS = (
    "incapsula",
    "cloudflare",
    "captcha",
    "challenge",
    "attention required!",
)

# Patrones que generan falsos positivos en la detección de WAF
# (ej. CDN legítimos que contienen subcadenas de los markers).
_WAF_FALSE_POSITIVE_PATTERNS: tuple[str, ...] = (
    r"cdnjs\.cloudflare\.com",
    r"cloudflare\.com/ajax/libs/",
    r"/_incapsula_resource",  # Script CDN legítimo del Senado, no WAF challenge
)

_TIMEOUT_MARKERS = (
    "timeout",
    "time out",
    "gateway timeout",
    "error 504",
)

_CACHE_MARKERS = (
    "cached",
    "cache inconsistent",
    "stale",
)

_BLOCKED_MARKERS = (
    "access denied",
    "403 forbidden",
    "blocked",
    "your request has been blocked",
    "request blocked",
    "service unavailable",
    "bad gateway",
    "ray id",
    "akamai",
    "invalid url",
    "error 403",
    "error 502",
    "error 503",
    "sucuri",
)


def _detect_waf(
    body: bytes | str, headers: dict | None = None
) -> XPIndeterminate | None:
    """Detecta subcadenas de WAF/cache/bloqueo en ``body``.

    Args:
        body: Cuerpo de respuesta en bytes o ya decodificado como str.
        headers: Headers HTTP (reservado para detección futura; no usado).

    Returns:
        ``XPIndeterminate`` con ``reason`` y ``detail`` si se detecta un
        patrón de bloqueo; de lo contrario ``None``.
    """
    if isinstance(body, bytes):
        try:
            text = body.decode("utf-8")
        except UnicodeDecodeError:
            text = body.decode("iso-8859-1", errors="replace")
    else:
        text = body

    lowered = text.lower()

    # Eliminar falsos positivos conocidos antes de evaluar markers
    for fp_pattern in _WAF_FALSE_POSITIVE_PATTERNS:
        lowered = re.sub(fp_pattern, "", lowered)

    if any(m in lowered for m in _WAF_CHALLENGE_MARKERS):
        return XPIndeterminate(
            reason="WAF_CHALLENGE",
            detail="Detectado patrón de WAF/captcha/challenge en body",
        )
    if any(m in lowered for m in _TIMEOUT_MARKERS):
        return XPIndeterminate(
            reason="TIMEOUT",
            detail="Detectado patrón de timeout/gateway timeout en body",
        )
    if any(m in lowered for m in _CACHE_MARKERS):
        return XPIndeterminate(
            reason="CACHE_INCONSISTENCY",
            detail="Detectado patrón de cache inconsistency en body",
        )
    if any(m in lowered for m in _BLOCKED_MARKERS):
        return XPIndeterminate(
            reason="BLOCKED",
            detail="Detectado patrón de bloqueo genérico en body",
        )

    return None


# ---------------------------------------------------------------------------
# Normalización de sentidos de voto
# ---------------------------------------------------------------------------

_SENTIDO_GLOBAL_MAP: dict[str, str] = {
    "a favor": "a_favor",
    "afavor": "a_favor",
    "favor": "a_favor",
    "pro": "a_favor",
    "si": "a_favor",
    "sí": "a_favor",
    "a_favor": "a_favor",
    "en contra": "en_contra",
    "encontra": "en_contra",
    "contra": "en_contra",
    "no": "en_contra",
    "en_contra": "en_contra",
    "abstencion": "abstencion",
    "abstención": "abstencion",
    "abstenciones": "abstencion",
    "abst": "abstencion",
    "ausente": "ausente",
    "ausentes": "ausente",
    "ausencia": "ausente",
    "no presente": "ausente",
    "novoto": "novoto",
    "no voto": "novoto",
    "no votó": "novoto",
    "presente": "presente",
}


def _normalize_sentido(raw_sentido: str, source_tag: str) -> str | None:
    """Normaliza una cadena de sentido de voto al vocabulario canónico.

    Args:
        raw_sentido: Texto crudo del sentido (p. ej. ``"A FAVOR"``).
        source_tag: Etiqueta de la fuente; afecta la heurística para
            ``"presente"`` (SITL/INFOPAL lo tratan como ``"a_favor"``).

    Returns:
        Valor canónico (``a_favor``, ``en_contra``, …) o ``None`` si no se
        reconoce.
    """
    cleaned = raw_sentido.strip().lower()
    cleaned = (
        cleaned.replace("ó", "o")
        .replace("í", "i")
        .replace("á", "a")
        .replace("é", "e")
        .replace("ú", "u")
    )

    # Heurística SITL: presente se interpreta como a_favor para fuentes
    # dip_sitl y dip_infopal (backwards compatibility).
    if cleaned == "presente" and (
        source_tag.startswith("dip_sitl") or source_tag.startswith("dip_infopal")
    ):
        return "a_favor"

    return _SENTIDO_GLOBAL_MAP.get(cleaned)


# ---------------------------------------------------------------------------
# Validación de conteos vs lista nominal
# ---------------------------------------------------------------------------

_VALID_SENTIDOS = frozenset(
    ("a_favor", "en_contra", "abstencion", "ausente", "novoto", "presente")
)


def _validate_counts_vs_nominal(counts: XPCounts, nominal_list: list[dict]) -> dict:
    """Compara conteos agregados contra la lista nominal.

    Args:
        counts: Conteos agregados (``XPCounts``).
        nominal_list: Lista de dicts con clave ``"sentido"``.

    Returns:
        Dict con ``ok``, ``expected``, ``actual`` y ``diff``.
    """
    expected: dict[str, int] = {s: 0 for s in _VALID_SENTIDOS}
    for item in nominal_list:
        sentido = item.get("sentido")
        if sentido in _VALID_SENTIDOS:
            expected[sentido] += 1

    actual: dict[str, int] = {s: counts.get(s, 0) for s in _VALID_SENTIDOS}

    diff: dict[str, int] = {s: expected[s] - actual[s] for s in _VALID_SENTIDOS}

    ok = all(v == 0 for v in diff.values())

    return {
        "ok": ok,
        "expected": expected,
        "actual": actual,
        "diff": diff,
    }
