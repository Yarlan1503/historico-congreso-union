"""Estrategia de scraping para SITL/INFOPAL (Cámara de Diputados) — paramétrico por legislatura."""

from __future__ import annotations

import logging
import re
import tomllib
from datetime import date
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from bs4 import BeautifulSoup

from scraper.engine import HTTPScraperEngine
from scraper.persistence import ScraperPersistence
from scraper.pipeline import process

logger = logging.getLogger(__name__)

_SOURCE_TAG = "dip_sitl"

# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

_CONFIG_PATH = Path(__file__).resolve().parent.parent / "config.toml"
_config_cache: dict | None = None


def _load_config() -> dict:
    """Carga la config TOML del scraper (con caché en módulo)."""
    global _config_cache
    if _config_cache is None:
        with open(_CONFIG_PATH, "rb") as fh:
            _config_cache = tomllib.load(fh)
    return _config_cache


def _get_urls_for_legislature(legislature: str) -> dict[str, str]:
    """Obtiene las URLs base para una legislatura desde config."""
    config = _load_config()
    leg_config = config.get("legislatures", {}).get(legislature, {})
    return leg_config.get("urls", {}).get("diputados", {})


def _get_periods_for_legislature(legislature: str) -> list[int]:
    """Obtiene los periodos SITL para una legislatura desde config."""
    config = _load_config()
    leg_config = config.get("legislatures", {}).get(legislature, {})
    return leg_config.get("periods", {}).get("diputados_sitl", [1, 3, 5])


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------


_DATE_RE = re.compile(
    r"(\d{1,2})\s+"
    r"(Enero|Febrero|Marzo|Abril|Mayo|Junio|Julio|Agosto|Septiembre|Octubre|Noviembre|Diciembre)"
    r"\s+(\d{4})",
    re.IGNORECASE,
)

_MESES: dict[str, int] = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
    "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
    "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
}


def _parse_spanish_date(text: str) -> date | None:
    """Convierte una fecha en español como ``'3 Septiembre 2024'`` a ``date``."""
    m = _DATE_RE.search(text)
    if not m:
        return None
    day = int(m.group(1))
    month = _MESES[m.group(2).lower()]
    year = int(m.group(3))
    try:
        return date(year, month, day)
    except ValueError:
        return None


def _extract_votacion_ids(body: bytes) -> dict[str, str | None]:
    """Extrae IDs de votación (``votaciont``) y su fecha asociada del índice por periodo.

    Recorre las filas de las tablas del HTML secuencialmente. Las filas que
    contienen una fecha en español (``D Mes YYYY``) con ≤3 celdas establecen
    la fecha vigente. Las filas con links ``votaciont=`` heredan esa fecha.

    Returns:
        Dict ``{votacion_id: fecha_str | None}`` ordenado por key.
    """
    result: dict[str, str | None] = {}
    try:
        text = body.decode("utf-8", errors="replace")
    except Exception:  # pragma: no cover
        text = body.decode("iso-8859-1", errors="replace")

    soup = BeautifulSoup(text, "html.parser")
    current_date: str | None = None
    processed_anchors: set[int] = set()

    for row in soup.find_all("tr"):
        cells = row.find_all(["td", "th"])
        row_text = row.get_text(" ", strip=True)

        # Check if this is a date row (≤3 cells and matches Spanish date)
        if len(cells) <= 3:
            m = _DATE_RE.search(row_text)
            if m:
                current_date = row_text
                continue

        # Extract votacion IDs from links in this row
        for anchor in row.find_all("a", href=True):
            processed_anchors.add(id(anchor))
            href = anchor["href"]
            if "votaciont=" not in href:
                continue
            parsed = urlparse(href)
            params = parse_qs(parsed.query)
            for val in params.get("votaciont", []):
                if val and (val not in result or current_date is not None):
                    result[val] = current_date

    # Fallback: anchors NOT inside any <tr> (bare links in body)
    # Only add if the ID was not already captured with a date above.
    for anchor in soup.find_all("a", href=True):
        if id(anchor) in processed_anchors:
            continue
        href = anchor["href"]
        if "votaciont=" not in href:
            continue
        parsed = urlparse(href)
        params = parse_qs(parsed.query)
        for val in params.get("votaciont", []):
            if val and val not in result:
                result[val] = None

    return dict(sorted(result.items()))


def _persist_process(
    persistence: ScraperPersistence,
    process_result,
    stats: dict,
) -> None:
    """Persiste un ``ProcessResult`` y actualiza contadores de ``stats``."""
    from f2.models import SourceTag

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


# ---------------------------------------------------------------------------
# Main scraper
# ---------------------------------------------------------------------------


def scrape_sitl(
    engine: HTTPScraperEngine,
    persistence: ScraperPersistence,
    legislature: str = "LXVI",
    since: date | None = None,
    max_votaciones: int | None = None,
    partidot_range: range | None = None,
) -> dict:
    """Ejecuta scraping completo de SITL/INFOPAL.

    Args:
        engine: Motor HTTP configurado.
        persistence: Capa de persistencia idempotente.
        legislature: Legislatura a scrapear (e.g. "LXVI", "LXV").
        since: Fecha límite inferior; votaciones anteriores se descartan
            después de capturar el agregado.
        max_votaciones: Límite de votaciones a procesar **por periodo**
            (útil para pruebas).
        partidot_range: Rango de IDs de partido para captura nominal.
            Default ``range(1, 31)``.

    Returns:
        Dict con métricas de la ejecución.
    """
    urls = _get_urls_for_legislature(legislature)
    if not urls:
        logger.error("No hay URLs configuradas para legislatura %s", legislature)
        return {
            "source": f"sitl_{legislature.lower()}",
            "error_fatal": f"Sin config para {legislature}",
        }

    index_url_tpl = urls.get("sitl_index", "")
    aggregate_url_tpl = urls.get("sitl_aggregate", "")
    nominal_url_tpl = urls.get("sitl_nominal", "")

    periods = _get_periods_for_legislature(legislature)

    if partidot_range is None:
        partidot_range = range(1, 31)

    stats: dict = {
        "source": f"sitl_{legislature.lower()}",
        "legislature": legislature,
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

    all_votacion_dates: dict[str, str | None] = {}

    # ------------------------------------------------------------------
    # 1. Descubrimiento por periodo
    # ------------------------------------------------------------------
    for periodo in periods:
        index_url = index_url_tpl.format(periodo=periodo)
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
            ids = dict(list(ids.items())[:max_votaciones])

        all_votacion_dates.update(ids)

    unique_ids = sorted(all_votacion_dates.keys())
    stats["votaciones_descubiertas"] = len(unique_ids)

    # ------------------------------------------------------------------
    # 2. Procesamiento por votación
    # ------------------------------------------------------------------
    for votacion_id in unique_ids:
        logger.info("Procesando votación %s", votacion_id)

        # 2a. Agregado
        agg_url = aggregate_url_tpl.format(votacion_id=votacion_id)
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

        # Propagar fecha desde el índice al vote_event
        fecha_str = all_votacion_dates.get(votacion_id)
        if fecha_str and agg_proc.vote_event:
            parsed_date = _parse_spanish_date(fecha_str)
            if parsed_date:
                agg_proc.vote_event["vote_date"] = parsed_date.isoformat()

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
            nom_url = nominal_url_tpl.format(partidot=partidot, votacion_id=votacion_id)
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

            # Propagar fecha desde el índice al vote_event nominal
            if fecha_str and nom_proc.vote_event:
                parsed_date_nom = _parse_spanish_date(fecha_str)
                if parsed_date_nom:
                    nom_proc.vote_event["vote_date"] = parsed_date_nom.isoformat()

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


def scrape_sitl_lxvi(
    engine: HTTPScraperEngine,
    persistence: ScraperPersistence,
    since: date | None = None,
    max_votaciones: int | None = None,
    partidot_range: range | None = None,
) -> dict:
    """Backward compatible wrapper — delegates to :func:`scrape_sitl` with legislature=LXVI."""
    return scrape_sitl(
        engine,
        persistence,
        legislature="LXVI",
        since=since,
        max_votaciones=max_votaciones,
        partidot_range=partidot_range,
    )
