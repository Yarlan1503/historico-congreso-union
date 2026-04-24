"""Puentes de transformación compartidos entre scraper (F1) e ingestor (F2).

Centraliza lógica de normalización que antes estaba duplicada en
``scraper/pipeline.py`` y ``f2/ingest_f1.py``.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any

from f1.parsers.xp_utils import (
    _normalize_sentido as _xp_normalize_sentido,
    _validate_counts_vs_nominal as _xp_validate_counts_vs_nominal,
)
from f2.models import Chamber

logger = logging.getLogger(__name__)

__all__ = [
    "build_counts",
    "infer_chamber",
    "map_source_tag",
    "normalize_sentido",
    "parse_date_heuristic",
    "validate_counts_vs_nominal",
]

# ---------------------------------------------------------------------------
# Fechas
# ---------------------------------------------------------------------------


def parse_date_heuristic(value: Any) -> date | None:
    """Intenta extraer un ``date`` de str/dict/date/datetime."""
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if not isinstance(value, str):
        return None
    value = value.strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    try:
        dt = datetime.fromisoformat(value)
        return dt.date()
    except ValueError:
        pass
    return None


# ---------------------------------------------------------------------------
# Cámara
# ---------------------------------------------------------------------------


def infer_chamber(source_tag: str) -> Chamber:
    """Infier la cámara a partir del ``source_tag`` (str)."""
    tag = source_tag.lower().strip()
    if tag.startswith("dip_"):
        return Chamber.DIPUTADOS
    if tag.startswith("sen_") or tag.startswith("senado_"):
        return Chamber.SENADO
    logger.warning("No se pudo inferir cámara para %s; default=senado", source_tag)
    return Chamber.SENADO


# ---------------------------------------------------------------------------
# Sentido
# ---------------------------------------------------------------------------


def normalize_sentido(raw: str, source_tag: str) -> str | None:
    """Normaliza una cadena de sentido de voto al vocabulario canónico.

    Delega en ``f1.parsers.xp_utils._normalize_sentido`` para garantizar
    consistencia con los parsers F1.
    """
    return _xp_normalize_sentido(raw, source_tag)


# ---------------------------------------------------------------------------
# Conteos
# ---------------------------------------------------------------------------

_SENTIDO_KEYS = ("a_favor", "en_contra", "abstencion", "ausente", "novoto", "presente")
_COUNT_KEYS = _SENTIDO_KEYS + ("total",)


def build_counts(parsed: dict[str, Any]) -> list[dict[str, Any]]:
    """Convierte ``counts`` o ``group_sentido`` de un dict parseado a lista plana.

    Cada elemento devuelto tiene las claves:
    ``group_name``, ``a_favor``, ``en_contra``, ``abstencion``,
    ``ausente``, ``novoto``, ``presente``, ``total``.

    Los sentidos faltantes defaultean a ``0``; ``total`` se calcula
    dinámicamente como la suma de los sentidos.
    """
    counts_list: list[dict[str, Any]] = []
    group_sentido = parsed.get("group_sentido")
    simple_counts = parsed.get("counts")

    if isinstance(group_sentido, dict):
        for group_name, group_counts in group_sentido.items():
            entry: dict[str, Any] = {"group_name": group_name}
            if isinstance(group_counts, dict):
                for k in _SENTIDO_KEYS:
                    entry[k] = group_counts.get(k, 0)
            else:
                for k in _SENTIDO_KEYS:
                    entry[k] = 0
            entry["total"] = sum(entry[k] for k in _SENTIDO_KEYS)
            counts_list.append(entry)
    elif isinstance(simple_counts, dict):
        entry = {"group_name": None}
        for k in _SENTIDO_KEYS:
            entry[k] = simple_counts.get(k, 0)
        entry["total"] = sum(entry[k] for k in _SENTIDO_KEYS)
        counts_list.append(entry)

    return counts_list


# ---------------------------------------------------------------------------
# Validación cruzada
# ---------------------------------------------------------------------------


def validate_counts_vs_nominal(counts: dict[str, Any], nominal: list[dict]) -> dict[str, Any]:
    """Compara conteos agregados contra la lista nominal.

    Delega en ``f1.parsers.xp_utils._validate_counts_vs_nominal``.
    """
    return _xp_validate_counts_vs_nominal(counts, nominal)


# ---------------------------------------------------------------------------
# Mapeo de source_tag
# ---------------------------------------------------------------------------


def map_source_tag(manifest_source_tag: str, packet_id: str) -> str:
    """Mapea ``source_tag`` crudo del manifest al string canónico.

    Raises:
        ValueError: si el tag no tiene mapeo conocido.
    """
    tag = manifest_source_tag.lower().strip()
    if tag == "dip_sitl":
        return "dip_sitl"
    if tag in ("senado_lxvi_ajax", "sen_lxvi_ajax"):
        return "sen_lxvi_ajax"
    if tag in ("senado_lxvi_html", "sen_lxvi_html"):
        return "sen_lxvi_html"
    if tag == "dip_gaceta":
        if "tabla" in packet_id.lower():
            return "dip_gaceta_tabla"
        return "dip_gaceta_post"
    raise ValueError(f"source_tag no mapeado: {manifest_source_tag!r}")
