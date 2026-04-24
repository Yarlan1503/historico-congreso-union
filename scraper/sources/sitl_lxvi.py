"""Estrategia de scraping para SITL/INFOPAL LXVI (Cámara de Diputados)."""

from __future__ import annotations

import logging
from datetime import date
from urllib.parse import parse_qs, urlparse

from bs4 import BeautifulSoup

from f2.models import SourceTag
from scraper.engine import HTTPScraperEngine
from scraper.persistence import ScraperPersistence
from scraper.pipeline import process

logger = logging.getLogger(__name__)

_INDEX_URL = "https://sitl.diputados.gob.mx/LXVI_leg/votacionesxperiodonplxvi.php?pert={periodo}"
_AGGREGATE_URL = (
    "https://sitl.diputados.gob.mx/LXVI_leg/estadistico_votacionnplxvi.php?votaciont={votacion_id}"
)
_NOMINAL_URL = (
    "https://sitl.diputados.gob.mx/LXVI_leg/listados_votacionesnplxvi.php"
    "?partidot={partidot}&votaciont={votacion_id}"
)

_SOURCE_TAG = "dip_sitl"
_PERIODOS_LXVI = [1, 3, 5, 6, 8]


def _extract_votacion_ids(body: bytes) -> list[str]:
    """Extrae IDs de votación (``votaciont``) del HTML del índice por periodo."""
    ids: set[str] = set()
    try:
        text = body.decode("utf-8", errors="replace")
    except Exception:  # pragma: no cover
        text = body.decode("iso-8859-1", errors="replace")

    soup = BeautifulSoup(text, "html.parser")
    for anchor in soup.find_all("a", href=True):
        href = anchor["href"]
        if "votaciont=" not in href:
            continue
        parsed = urlparse(href)
        params = parse_qs(parsed.query)
        for val in params.get("votaciont", []):
            if val:
                ids.add(val)
    return sorted(ids)


def _persist_process(
    persistence: ScraperPersistence,
    process_result,
    stats: dict,
) -> None:
    """Persiste un ``ProcessResult`` y actualiza contadores de ``stats``."""
    try:
        result = persistence.persist(process_result, SourceTag.DIP_SITL)
    except Exception as exc:
        logger.exception("Error en persistencia")
        stats["errores"].append(str(exc))
        return

    if result.get("asset_inserted"):
        stats["assets_insertados"] += 1
    else:
        stats["assets_skipped"] += 1

    if result.get("event_inserted"):
        stats["vote_events_insertados"] += 1
    else:
        stats["vote_events_existentes"] += 1

    stats["casts_insertados"] += result.get("casts_inserted", 0)
    stats["counts_insertados"] += result.get("counts_inserted", 0)


def scrape_sitl_lxvi(
    engine: HTTPScraperEngine,
    persistence: ScraperPersistence,
    since: date | None = None,
    max_votaciones: int | None = None,
    partidot_range: range | None = None,
) -> dict:
    """Ejecuta scraping completo de SITL/INFOPAL LXVI.

    Args:
        engine: Motor HTTP configurado.
        persistence: Capa de persistencia idempotente.
        since: Fecha límite inferior; votaciones anteriores se descartan
            después de capturar el agregado.
        max_votaciones: Límite de votaciones a procesar **por periodo**
            (útil para pruebas).
        partidot_range: Rango de IDs de partido para captura nominal.
            Default ``range(1, 31)``.

    Returns:
        Dict con métricas de la ejecución.
    """
    if partidot_range is None:
        partidot_range = range(1, 31)

    stats: dict = {
        "source": "sitl_lxvi",
        "periodos_scrapeados": [],
        "votaciones_descubiertas": 0,
        "votaciones_procesadas": 0,
        "assets_insertados": 0,
        "assets_skipped": 0,
        "vote_events_insertados": 0,
        "vote_events_existentes": 0,
        "casts_insertados": 0,
        "counts_insertados": 0,
        "waf_detectados": 0,
        "indeterminates": 0,
        "errores": [],
    }

    all_votacion_ids: list[str] = []

    # ------------------------------------------------------------------
    # 1. Descubrimiento por periodo
    # ------------------------------------------------------------------
    for periodo in _PERIODOS_LXVI:
        index_url = _INDEX_URL.format(periodo=periodo)
        logger.info("Descubriendo periodo %s → %s", periodo, index_url)

        try:
            idx_fetch = engine.fetch(index_url, method="GET", source_tag=_SOURCE_TAG)
        except Exception as exc:
            logger.exception("Fetch error índice periodo %s", periodo)
            stats["errores"].append(f"periodo={periodo} fetch error: {exc}")
            continue

        if idx_fetch.waf_detected:
            logger.warning("WAF detectado en índice periodo %s", periodo)
            stats["waf_detectados"] += 1
            continue

        try:
            ids = _extract_votacion_ids(idx_fetch.body)
        except Exception as exc:
            logger.exception("Parse error índice periodo %s", periodo)
            stats["errores"].append(f"periodo={periodo} parse error: {exc}")
            continue

        logger.info("Periodo %s: %s votaciones descubiertas", periodo, len(ids))
        stats["periodos_scrapeados"].append(periodo)

        if max_votaciones is not None:
            ids = ids[:max_votaciones]

        all_votacion_ids.extend(ids)

    unique_ids = sorted(set(all_votacion_ids))
    stats["votaciones_descubiertas"] = len(unique_ids)

    # ------------------------------------------------------------------
    # 2. Procesamiento por votación
    # ------------------------------------------------------------------
    for votacion_id in unique_ids:
        logger.info("Procesando votación %s", votacion_id)

        # 2a. Agregado
        agg_url = _AGGREGATE_URL.format(votacion_id=votacion_id)
        try:
            agg_fetch = engine.fetch(agg_url, method="GET", source_tag=_SOURCE_TAG)
        except Exception as exc:
            logger.exception("Fetch error agregado votación %s", votacion_id)
            stats["errores"].append(f"votacion={votacion_id} agg fetch error: {exc}")
            continue

        if agg_fetch.waf_detected:
            logger.warning("WAF detectado en agregado votación %s", votacion_id)
            stats["waf_detectados"] += 1
            continue

        agg_proc = process(agg_fetch, _SOURCE_TAG)

        if agg_proc.classification == "INDETERMINATE":
            logger.warning(
                "INDETERMINATE agregado votación %s: %s",
                votacion_id,
                agg_proc.parser_errors,
            )
            stats["indeterminates"] += 1
            _persist_process(persistence, agg_proc, stats)
            continue

        # Filtrado por fecha
        vote_date = None
        if agg_proc.vote_event:
            vote_date = agg_proc.vote_event.get("vote_date")
        if since and vote_date and vote_date < since:
            logger.info(
                "Saltando votación %s (fecha %s < %s)",
                votacion_id,
                vote_date,
                since,
            )
            continue

        stats["votaciones_procesadas"] += 1

        # 2b. Nominal por partido
        nominal_procs: list = []
        for partidot in partidot_range:
            nom_url = _NOMINAL_URL.format(partidot=partidot, votacion_id=votacion_id)
            try:
                nom_fetch = engine.fetch(nom_url, method="GET", source_tag=_SOURCE_TAG)
            except Exception as exc:
                logger.exception(
                    "Fetch error nominal votación %s partidot %s",
                    votacion_id,
                    partidot,
                )
                stats["errores"].append(
                    f"votacion={votacion_id} partidot={partidot} fetch error: {exc}"
                )
                continue

            if nom_fetch.waf_detected:
                logger.warning(
                    "WAF detectado en nominal votación %s partidot %s",
                    votacion_id,
                    partidot,
                )
                stats["waf_detectados"] += 1
                continue

            nom_proc = process(nom_fetch, _SOURCE_TAG)

            if nom_proc.classification == "INDETERMINATE":
                logger.warning(
                    "INDETERMINATE nominal votación %s partidot %s: %s",
                    votacion_id,
                    partidot,
                    nom_proc.parser_errors,
                )
                stats["indeterminates"] += 1
                _persist_process(persistence, nom_proc, stats)
                continue

            # Saltar si no hay datos nominales
            if not nom_proc.casts:
                logger.debug(
                    "Sin nominal votación %s partidot %s; saltando",
                    votacion_id,
                    partidot,
                )
                continue

            nominal_procs.append(nom_proc)

        # Validación cross-asset: agregado vs nominal unificado
        if agg_proc.parsed_data and agg_proc.parsed_data.get("counts") and nominal_procs:
            from shared.transform_bridge import validate_counts_vs_nominal

            counts_dict = agg_proc.parsed_data["counts"]
            all_nominal = [
                {"sentido": cast["sentido"]} for proc in nominal_procs for cast in proc.casts
            ]
            if all_nominal:
                validation = validate_counts_vs_nominal(counts_dict, all_nominal)
                if not validation.get("ok", True):
                    logger.warning(
                        "Counts vs nominal mismatch en votación %s: %s", votacion_id, validation
                    )
                    stats["indeterminates"] += 1
                    # Marcar votación completa como INDETERMINATE
                    agg_proc.classification = "INDETERMINATE"
                    agg_proc.parser_errors.append(f"Counts vs nominal mismatch: {validation}")
                    # Descartar nominales
                    nominal_procs = []

        _persist_process(persistence, agg_proc, stats)

        for nom_proc in nominal_procs:
            _persist_process(persistence, nom_proc, stats)

    return stats
