"""Estrategia de scraping para Senado LXVI (portal actual)."""

from __future__ import annotations

import logging
from datetime import date

from scraper import pipeline
from scraper._types import FetchResult, ProcessResult
from f2.models import SourceTag

logger = logging.getLogger(__name__)

BASE_URL = "https://www.senado.gob.mx"


def _extract_vote_date(process_result: ProcessResult) -> date | None:
    """Extrae la fecha de votación de un ProcessResult exitoso."""
    if process_result.vote_event is not None:
        vd = process_result.vote_event.get("vote_date")
        if isinstance(vd, date):
            return vd
    parsed = process_result.parsed_data
    if isinstance(parsed, dict):
        metadata = parsed.get("metadata") or parsed.get("meta", {})
        if isinstance(metadata, dict):
            for key in ("fecha", "date", "vote_date"):
                val = metadata.get(key)
                if isinstance(val, date):
                    return val
    return None


def _is_waf(fetch_result: FetchResult, process_result: ProcessResult) -> bool:
    """Determina si el resultado es INDETERMINATE por WAF."""
    if fetch_result.waf_detected:
        return True
    if process_result.classification == "INDETERMINATE":
        parsed = process_result.parsed_data
        if isinstance(parsed, dict):
            reason = str(parsed.get("reason", "")).lower()
            detail = str(parsed.get("detail", "")).lower()
            if any(k in reason or k in detail for k in ("waf", "incapsula", "cloudflare", "akamai", "bot")):
                return True
        for err in process_result.parser_errors:
            err_lower = err.lower()
            if any(k in err_lower for k in ("waf", "incapsula", "cloudflare", "akamai", "bot")):
                return True
    return False


def _is_not_found(process_result: ProcessResult) -> bool:
    """Determina si el resultado indica que la votación no existe."""
    if process_result.fetch_result.status_code == 404:
        return True
    if process_result.classification == "INDETERMINATE":
        parsed = process_result.parsed_data
        if isinstance(parsed, dict):
            detail = str(parsed.get("detail", "")).lower()
            if "estructura no reconocida" in detail or "no reconocido" in detail:
                return True
        for err in process_result.parser_errors:
            err_lower = err.lower()
            if (
                "estructura no reconocida" in err_lower
                or "no devolvió counts, nominal ni group_sentido" in err_lower
                or "no reconocido" in err_lower
            ):
                return True
    return False


def _should_persist(process_result: ProcessResult, since: date | None) -> bool:
    """Determina si se debe persistir según filtro de fecha."""
    if since is None:
        return True
    vote_date = _extract_vote_date(process_result)
    if vote_date is None:
        return True
    return vote_date >= since


def _update_persist_stats(pr: dict, stats: dict, *, is_success: bool = False) -> None:
    """Actualiza contadores de persistencia desde un resultado de ``persist()``."""
    if pr.get("asset_inserted"):
        stats["assets_insertados"] += 1
    else:
        stats["assets_skipped"] += 1
    if is_success:
        if pr.get("event_inserted"):
            stats["vote_events_insertados"] += 1
        else:
            stats["vote_events_existentes"] += 1
    stats["casts_insertados"] += pr.get("casts_inserted", 0)
    stats["counts_insertados"] += pr.get("counts_inserted", 0)


def scrape_senado_lxvi(
    engine,
    persistence,
    since: date | None = None,
    id_range: range | None = None,
    break_on_consecutive_not_found: int = 20,
) -> dict:
    """Scrapea votaciones del Senado LXVI.

    Itera sobre un rango de IDs de votación intentando primero el endpoint
    AJAX (fuente canónica) y, si falla por razones no relacionadas a WAF,
    recurre a la página HTML como fallback.

    Args:
        engine: Instancia de ``HTTPScraperEngine``.
        persistence: Instancia de ``ScraperPersistence``.
        since: Fecha mínima para persistir votaciones. Si ``None`` no filtra.
        id_range: Rango de IDs a iterar. Default ``range(1, 5001)``.
        break_on_consecutive_not_found: Umbral de IDs consecutivos no
            encontrados (404 o estructura no reconocida) para detener el
            scraping temprano.

    Returns:
        Dict con estadísticas de la ejecución.
    """
    if id_range is None:
        id_range = range(1, 5001)

    id_range_desc = (
        list(id_range)
        if len(id_range) <= 20
        else f"{id_range.start}..{id_range.stop - 1}"
    )

    stats = {
        "source": "senado_lxvi",
        "id_range": id_range_desc,
        "ids_procesados": 0,
        "ajax_success": 0,
        "ajax_indeterminate": 0,
        "ajax_fail": 0,
        "html_fallback_success": 0,
        "html_fallback_indeterminate": 0,
        "html_fallback_fail": 0,
        "assets_insertados": 0,
        "assets_skipped": 0,
        "vote_events_insertados": 0,
        "vote_events_existentes": 0,
        "casts_insertados": 0,
        "counts_insertados": 0,
        "waf_detectados": 0,
        "consecutive_not_found_break": False,
        "errores": [],
    }

    consecutive_not_found = 0

    for vid in id_range:
        stats["ids_procesados"] += 1
        logger.info("Procesando votación ID %s", vid)

        # ------------------------------------------------------------------
        # AJAX primario
        # ------------------------------------------------------------------
        ajax_url = (
            f"{BASE_URL}/66/app/votaciones/functions/viewTableVot.php"
            f"?action=ajax&cell=1&order=DESC&votacion={vid}&q="
        )

        fetch_ajax = None
        process_ajax = None

        try:
            fetch_ajax = engine.fetch(
                ajax_url, method="GET", source_tag="sen_lxvi_ajax"
            )
        except Exception as exc:
            logger.exception("Excepción en fetch AJAX ID %s", vid)
            stats["errores"].append(f"ID {vid} AJAX fetch exc: {exc}")

        if fetch_ajax is not None:
            try:
                process_ajax = pipeline.process(
                    fetch_ajax, source_tag="sen_lxvi_ajax"
                )
            except Exception as exc:
                logger.exception("Excepción en pipeline AJAX ID %s", vid)
                stats["errores"].append(f"ID {vid} AJAX pipeline exc: {exc}")

        ajax_finalized = False

        if fetch_ajax is not None and process_ajax is not None:
            if _is_waf(fetch_ajax, process_ajax):
                # Guardar evidencia WAF; no intentar fallback
                try:
                    pr = persistence.persist(
                        process_ajax, SourceTag.SEN_LXVI_AJAX
                    )
                    _update_persist_stats(pr, stats, is_success=False)
                except Exception as exc:
                    logger.exception("Persist WAF AJAX ID %s error", vid)
                    stats["errores"].append(
                        f"ID {vid} AJAX WAF persist: {exc}"
                    )
                stats["ajax_indeterminate"] += 1
                stats["waf_detectados"] += 1
                consecutive_not_found = 0
                ajax_finalized = True

            elif process_ajax.classification == "SUCCESS":
                if _should_persist(process_ajax, since):
                    try:
                        pr = persistence.persist(
                            process_ajax, SourceTag.SEN_LXVI_AJAX
                        )
                        _update_persist_stats(pr, stats, is_success=True)
                    except Exception as exc:
                        logger.exception("Persist AJAX SUCCESS ID %s error", vid)
                        stats["errores"].append(
                            f"ID {vid} AJAX persist: {exc}"
                        )
                else:
                    logger.info("ID %s AJAX omitido por filtro since", vid)
                stats["ajax_success"] += 1
                consecutive_not_found = 0
                ajax_finalized = True

            else:
                # AJAX fallido: persistir evidencia
                try:
                    pr = persistence.persist(
                        process_ajax, SourceTag.SEN_LXVI_AJAX
                    )
                    _update_persist_stats(pr, stats, is_success=False)
                except Exception as exc:
                    logger.exception("Persist AJAX failed ID %s error", vid)
                    stats["errores"].append(
                        f"ID {vid} AJAX failed persist: {exc}"
                    )

                if process_ajax.classification == "INDETERMINATE":
                    stats["ajax_indeterminate"] += 1
                else:
                    stats["ajax_fail"] += 1

                if fetch_ajax.status_code == 404:
                    consecutive_not_found += 1
                    ajax_finalized = True  # no tiene sentido fallback

        if ajax_finalized:
            if consecutive_not_found >= break_on_consecutive_not_found:
                logger.warning(
                    "Break tras %s IDs consecutivos no encontrados (umbral %s)",
                    consecutive_not_found,
                    break_on_consecutive_not_found,
                )
                stats["consecutive_not_found_break"] = True
                break
            continue

        # ------------------------------------------------------------------
        # HTML fallback
        # ------------------------------------------------------------------
        html_url = f"{BASE_URL}/66/votacion/{vid}"

        fetch_html = None
        process_html = None

        try:
            fetch_html = engine.fetch(
                html_url, method="GET", source_tag="sen_lxvi_html"
            )
        except Exception as exc:
            logger.exception("Excepción en fetch HTML ID %s", vid)
            stats["errores"].append(f"ID {vid} HTML fetch exc: {exc}")

        if fetch_html is not None:
            try:
                process_html = pipeline.process(
                    fetch_html, source_tag="sen_lxvi_html"
                )
            except Exception as exc:
                logger.exception("Excepción en pipeline HTML ID %s", vid)
                stats["errores"].append(
                    f"ID {vid} HTML pipeline exc: {exc}"
                )

        html_finalized = False

        if fetch_html is not None and process_html is not None:
            if _is_waf(fetch_html, process_html):
                try:
                    pr = persistence.persist(
                        process_html, SourceTag.SEN_LXVI_HTML
                    )
                    _update_persist_stats(pr, stats, is_success=False)
                except Exception as exc:
                    logger.exception("Persist WAF HTML ID %s error", vid)
                    stats["errores"].append(
                        f"ID {vid} HTML WAF persist: {exc}"
                    )
                stats["html_fallback_indeterminate"] += 1
                stats["waf_detectados"] += 1
                consecutive_not_found = 0
                html_finalized = True

            elif process_html.classification == "SUCCESS":
                if _should_persist(process_html, since):
                    try:
                        pr = persistence.persist(
                            process_html, SourceTag.SEN_LXVI_HTML
                        )
                        _update_persist_stats(pr, stats, is_success=True)
                    except Exception as exc:
                        logger.exception(
                            "Persist HTML SUCCESS ID %s error", vid
                        )
                        stats["errores"].append(
                            f"ID {vid} HTML persist: {exc}"
                        )
                else:
                    logger.info("ID %s HTML omitido por filtro since", vid)
                stats["html_fallback_success"] += 1
                consecutive_not_found = 0
                html_finalized = True

            else:
                # HTML fallido
                try:
                    pr = persistence.persist(
                        process_html, SourceTag.SEN_LXVI_HTML
                    )
                    _update_persist_stats(pr, stats, is_success=False)
                except Exception as exc:
                    logger.exception("Persist HTML failed ID %s error", vid)
                    stats["errores"].append(
                        f"ID {vid} HTML failed persist: {exc}"
                    )

                if process_html.classification == "INDETERMINATE":
                    stats["html_fallback_indeterminate"] += 1
                else:
                    stats["html_fallback_fail"] += 1

                if _is_not_found(process_html):
                    consecutive_not_found += 1
                else:
                    consecutive_not_found = 0
                html_finalized = True

        if not html_finalized:
            # Error de infraestructura (fetch o pipeline lanzó excepción)
            stats["html_fallback_fail"] += 1
            consecutive_not_found = 0

        if consecutive_not_found >= break_on_consecutive_not_found:
            logger.warning(
                "Break tras %s IDs consecutivos no encontrados (umbral %s)",
                consecutive_not_found,
                break_on_consecutive_not_found,
            )
            stats["consecutive_not_found_break"] = True
            break

    return stats
