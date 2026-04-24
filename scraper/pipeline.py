"""Pipeline de procesamiento: parseo, normalización, validación cruzada.

Conecta los parsers refactorizados de F1 con los modelos productivos de F2.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from pathlib import Path
from typing import Any

from f1.parsers import xp_diputados_gaceta, xp_diputados_sitl, xp_senado_lxvi
from f2.models import AssetRole
from scraper._types import FetchResult, ProcessResult
from shared.transform_bridge import (
    build_counts,
    infer_chamber,
    normalize_sentido,
    parse_date_heuristic,
    validate_counts_vs_nominal,
)

logger = logging.getLogger(__name__)

PARSER_VERSION = "scraper_0.1.0"

# ---------------------------------------------------------------------------
# Mapeo de fuentes a parsers
# ---------------------------------------------------------------------------

def get_parser_module(source_tag: str) -> tuple[Callable, str]:
    """Devuelve (función_parser, suffix) para un ``source_tag``."""
    mapping: dict[str, tuple[Callable, str]] = {
        "dip_sitl": (xp_diputados_sitl.parse_response, "dip_sitl"),
        "dip_infopal": (xp_diputados_sitl.parse_response, "dip_sitl"),
        "dip_gaceta_tabla": (xp_diputados_gaceta.parse_tabla_agregada, "dip_gaceta_tabla"),
        "dip_gaceta_post": (xp_diputados_gaceta.parse_response, "dip_gaceta_post"),
        "sen_lxvi_ajax": (xp_senado_lxvi.parse_response, "sen_lxvi_ajax"),
        "senado_lxvi_ajax": (xp_senado_lxvi.parse_response, "sen_lxvi_ajax"),
        "sen_lxvi_html": (xp_senado_lxvi.parse_response, "sen_lxvi_html"),
        "senado_lxvi_html": (xp_senado_lxvi.parse_response, "sen_lxvi_html"),
    }
    try:
        return mapping[source_tag]
    except KeyError as exc:
        raise ValueError(f"source_tag no reconocido: {source_tag}") from exc


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extract_metadata(parsed: dict[str, Any]) -> dict[str, Any]:
    meta = dict(parsed.get("metadata", {}))
    meta.update(parsed.get("meta", {}))
    return meta


def _build_source_asset(fetch_result: FetchResult, source_tag: str) -> dict[str, Any]:
    content_type = fetch_result.headers.get("content-type") or fetch_result.headers.get("Content-Type")
    return {
        "source_tag": source_tag,
        "url": fetch_result.url,
        "method": fetch_result.method.upper(),
        "response_body_hash": fetch_result.sha256_body,
        "response_headers_hash": fetch_result.sha256_headers,
        "status_code": fetch_result.status_code,
        "content_type": content_type,
        "encoding": None,
        "captured_at": fetch_result.timestamp,
        "waf_detected": fetch_result.waf_detected,
        "cache_detected": fetch_result.cache_detected,
        "repetition_num": 1,
        "run_id": None,
        "raw_body_path": Path("."),
    }


def _build_vote_event(fetch_result: FetchResult, parsed: dict[str, Any]) -> dict[str, Any]:
    metadata = _extract_metadata(parsed)
    vote_date = None
    for key in ("fecha", "date", "vote_date", "publication_date"):
        if key in metadata:
            vote_date = parse_date_heuristic(metadata[key])
            if vote_date is not None:
                break
    return {
        "chamber": infer_chamber(parsed.get("source_tag", "")),
        "legislature": "LXVI",
        "vote_date": vote_date,
        "title": metadata.get("titulo") or metadata.get("title") or metadata.get("asunto"),
        "subject": metadata.get("asunto") or metadata.get("dictamen") or metadata.get("subject"),
        "source_url": fetch_result.url,
        "metadata_json": metadata,
    }


def _infer_asset_role(source_tag: str, parsed: dict[str, Any]) -> AssetRole:
    nominal = parsed.get("nominal")
    counts = parsed.get("counts")
    group_sentido = parsed.get("group_sentido")

    if source_tag == "dip_sitl":
        if nominal:
            return AssetRole.PRIMARY_NOMINAL
        if counts:
            return AssetRole.PRIMARY_AGGREGATE
    if source_tag == "sen_lxvi_ajax":
        if nominal:
            return AssetRole.PRIMARY_NOMINAL
    if source_tag == "dip_gaceta_tabla":
        if group_sentido:
            return AssetRole.PRIMARY_AGGREGATE
    if source_tag == "dip_gaceta_post":
        return AssetRole.METADATA
    if source_tag == "sen_lxvi_html":
        return AssetRole.TRIANGULATION
    return AssetRole.METADATA


def _build_casts(parsed: dict[str, Any], source_tag: str) -> list[dict[str, Any]]:
    casts: list[dict[str, Any]] = []
    nominal = parsed.get("nominal")
    if not isinstance(nominal, list):
        return casts

    for row in nominal:
        if not isinstance(row, dict):
            continue
        raw_sentido = row.get("sentido", "")
        sentido = normalize_sentido(raw_sentido, source_tag)
        if sentido is None:
            continue

        # SITL/INFOPAL: partidot es el nombre del legislador (campo confuso)
        if source_tag.startswith("dip_sitl") or source_tag.startswith("dip_infopal"):
            name = row.get("partidot", "")
        else:
            name = row.get("nombre", "")

        group = row.get("grupo", "")

        casts.append({
            "legislator_name": name,
            "legislator_group": group if group else None,
            "sentido": sentido,
            "region": None,
            "raw_row_json": dict(row),
        })
    return casts


# ---------------------------------------------------------------------------
# Procesamiento principal
# ---------------------------------------------------------------------------

def process(fetch_result: FetchResult, source_tag: str) -> ProcessResult:
    parser_func, suffix = get_parser_module(source_tag)
    parser_version = f"{PARSER_VERSION}_{suffix}"
    parsed: dict[str, Any] | None = None
    classification = "SUCCESS"
    parser_errors: list[str] = []

    try:
        result = parser_func(fetch_result.body, source_tag, parser_version)
    except Exception as exc:
        logger.exception("Parser exception for %s", source_tag)
        return ProcessResult(
            fetch_result=fetch_result,
            classification="FAIL",
            source_asset=_build_source_asset(fetch_result, source_tag),
            parser_errors=[str(exc)],
        )

    if isinstance(result, dict) and "reason" in result:
        # Indeterminate
        classification = "INDETERMINATE"
        parser_errors.append(result.get("reason", "UNKNOWN"))
        return ProcessResult(
            fetch_result=fetch_result,
            classification=classification,
            source_asset=_build_source_asset(fetch_result, source_tag),
            parser_errors=parser_errors,
            parsed_data=result if isinstance(result, dict) else None,
        )

    if not isinstance(result, dict):
        classification = "INDETERMINATE"
        parser_errors.append("resultado del parser no es un dict")
        return ProcessResult(
            fetch_result=fetch_result,
            classification=classification,
            source_asset=_build_source_asset(fetch_result, source_tag),
            parser_errors=parser_errors,
        )

    parsed = result
    if "counts" not in parsed and "nominal" not in parsed and "group_sentido" not in parsed:
        classification = "INDETERMINATE"
        parser_errors.append("parser no devolvió counts, nominal ni group_sentido")
        return ProcessResult(
            fetch_result=fetch_result,
            classification=classification,
            source_asset=_build_source_asset(fetch_result, source_tag),
            parser_errors=parser_errors,
            parsed_data=parsed,
        )

    # Validación cruzada counts vs nominal (solo si ambos tienen datos)
    counts = parsed.get("counts")
    nominal = parsed.get("nominal")
    if counts and nominal:
        validation = validate_counts_vs_nominal(counts, nominal)
        if not validation.get("ok", True):
            logger.warning(
                "Counts vs nominal mismatch (%s): %s",
                source_tag,
                validation,
            )
            classification = "INDETERMINATE"
            parser_errors.append(f"Counts vs nominal mismatch: {validation}")
            return ProcessResult(
                fetch_result=fetch_result,
                classification=classification,
                source_asset=_build_source_asset(fetch_result, source_tag),
                parser_errors=parser_errors,
                parsed_data=parsed,
            )

    source_asset = _build_source_asset(fetch_result, source_tag)
    vote_event = _build_vote_event(fetch_result, parsed)
    vote_event_asset = {
        "asset_role": _infer_asset_role(source_tag, parsed),
    }
    casts = _build_casts(parsed, source_tag)
    counts_list = build_counts(parsed)

    return ProcessResult(
        fetch_result=fetch_result,
        classification=classification,
        source_asset=source_asset,
        vote_event=vote_event,
        vote_event_asset=vote_event_asset,
        casts=casts,
        counts=counts_list,
        parsed_data=parsed,
        parser_errors=parser_errors,
    )
