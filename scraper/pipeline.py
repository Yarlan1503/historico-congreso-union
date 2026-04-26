"""Pipeline de procesamiento: parseo, normalización, validación cruzada.

Conecta los parsers refactorizados de F1 con los modelos productivos de F2.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from pathlib import Path
from typing import Any

import scraper._builtin_sources  # noqa: F401 — registra fuentes built-in
from f2.models import AssetRole
from scraper._types import FetchResult, ProcessResult
from scraper.source_registry import get_parser
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
    try:
        return get_parser(source_tag)
    except ValueError as exc:
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


def _infer_legislature(source_tag: str, url: str | None = None) -> str:
    """Infier la legislatura desde URL, registry o heurística del tag."""
    # Priority 1: URL pattern (most reliable for multi-legislature sources)
    if url:
        import re
        m = re.search(r'/((?:LX{0,2}(?:IV|V?I{0,3}))_leg)/', url, re.IGNORECASE)
        if m:
            leg_candidate = m.group(1).replace('_leg', '').upper()
            if leg_candidate in ("LXVI", "LXV", "LXIV", "LXIII", "LXII", "LXI", "LX"):
                return leg_candidate

    # Priority 2: Registry
    from scraper.source_registry import get_source
    try:
        info = get_source(source_tag)
        return info.legislature
    except ValueError:
        pass

    # Priority 3: Tag pattern fallback
    tag_upper = source_tag.upper()
    for leg in ("LXVI", "LXV", "LXIV", "LXIII", "LXII", "LXI", "LX"):
        if leg in tag_upper:
            return leg

    # Default: LXVI para backward compatibility con tags existentes
    logger.warning("No se pudo inferir legislatura para '%s'; default=LXVI", source_tag)
    return "LXVI"


def _build_vote_event(
    fetch_result: FetchResult,
    parsed: dict[str, Any],
    source_tag: str = "",
) -> dict[str, Any]:
    metadata = _extract_metadata(parsed)
    vote_date = None
    for key in ("fecha", "date", "vote_date", "publication_date"):
        if key in metadata:
            vote_date = parse_date_heuristic(metadata[key])
            if vote_date is not None:
                break

    # Infer legislature from registry
    legislature = _infer_legislature(source_tag, url=fetch_result.url)

    return {
        "chamber": infer_chamber(source_tag),
        "legislature": legislature,
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

    # Pattern-based inference
    tag = source_tag.lower()

    # SITL/INFOPAL pattern: aggregate and nominal in separate assets
    if "sitl" in tag:
        if nominal:
            return AssetRole.PRIMARY_NOMINAL
        if counts:
            return AssetRole.PRIMARY_AGGREGATE

    # Senado AJAX pattern
    if "_ajax" in tag:
        if nominal:
            return AssetRole.PRIMARY_NOMINAL
        return AssetRole.METADATA

    # Gaceta patterns
    if "gaceta" in tag and "tabla" in tag:
        if group_sentido:
            return AssetRole.PRIMARY_AGGREGATE
    if "gaceta" in tag and "post" in tag:
        return AssetRole.METADATA

    # Senado HTML pattern
    if "_html" in tag and "sen" in tag:
        return AssetRole.TRIANGULATION

    # Generic: if has nominal → PRIMARY_NOMINAL, if has counts → PRIMARY_AGGREGATE
    if nominal:
        return AssetRole.PRIMARY_NOMINAL
    if counts or group_sentido:
        return AssetRole.PRIMARY_AGGREGATE

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

    # Validación cruzada counts vs nominal (solo si ambos tienen datos, y no para SITL/INFOPAL
    # porque agregado y nominal están en assets separados)
    counts = parsed.get("counts")
    nominal = parsed.get("nominal")
    if counts and nominal and source_tag not in ("dip_sitl", "dip_infopal"):
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
    vote_event = _build_vote_event(fetch_result, parsed, source_tag=source_tag)
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
