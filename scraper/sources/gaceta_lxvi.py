"""Estrategia de scraping para Gaceta Parlamentaria LXVI (Diputados)."""

from __future__ import annotations

import json
import logging
from datetime import date

from f2.models import SourceTag
from scraper import pipeline
from scraper.engine import HTTPScraperEngine
from scraper.persistence import ScraperPersistence

logger = logging.getLogger(__name__)

BASE_URL = "https://gaceta.diputados.gob.mx/Gaceta/Votaciones/66/"


def _fetch_tabla(
    engine: HTTPScraperEngine,
    numero: int,
) -> tuple[str, object]:
    """Intenta descargar la tabla agregada, probando variantes de URL.

    Returns:
        (url_usada, fetch_result)
    """
    primary_url = f"{BASE_URL}tabla2or1-{numero}.php3"
    fetch_result = engine.fetch(primary_url, method="GET", source_tag="dip_gaceta_tabla")

    # Fallback a variante si el primero es INDETERMINATE por 404 / vacío
    if fetch_result.status_code == 404 or (
        fetch_result.status_code == 200
        and fetch_result.body
        and b"tabla agregada no reconocida" in fetch_result.body
    ):
        variant_url = f"{BASE_URL}tabla2or1-{numero}or1.php3"
        logger.debug("Variante fallback para tabla %s: %s", numero, variant_url)
        fetch_result = engine.fetch(variant_url, method="GET", source_tag="dip_gaceta_tabla")
        return variant_url, fetch_result

    return primary_url, fetch_result


def scrape_gaceta_lxvi(
    engine: HTTPScraperEngine,
    persistence: ScraperPersistence,
    since: date | None = None,
    tabla_range: range | None = None,
) -> dict:
    """Captura tablas agregadas y POSTs nominales de la Gaceta Parlamentaria LXVI.

    Args:
        engine: Instancia de HTTPScraperEngine.
        persistence: Instancia de ScraperPersistence.
        since: Fecha límite inferior. Tablas con fecha anterior no disparan POSTs.
        tabla_range: Rango de números de tabla a explorar (default 1-200).

    Returns:
        Dict con métricas de la corrida.
    """
    if tabla_range is None:
        tabla_range = range(1, 201)

    stats = {
        "source": "gaceta_lxvi",
        "tablas_descubiertas": 0,
        "tablas_procesadas": 0,
        "post_nominales_enviados": 0,
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

    for numero in tabla_range:
        stats["tablas_descubiertas"] += 1
        logger.info("Procesando tabla número %s", numero)

        try:
            tabla_url, fetch_result = _fetch_tabla(engine, numero)
        except Exception as exc:
            logger.exception("Error de red en tabla %s", numero)
            stats["errores"].append(f"tabla {numero} red: {exc}")
            continue

        if fetch_result.waf_detected:
            logger.warning("WAF detectado en tabla %s (%s)", numero, tabla_url)
            stats["waf_detectados"] += 1
            continue

        process_result = pipeline.process(fetch_result, source_tag="dip_gaceta_tabla")

        if process_result.classification == "INDETERMINATE":
            logger.info(
                "Tabla %s INDETERMINATE: %s",
                numero,
                process_result.parser_errors,
            )
            stats["indeterminates"] += 1
            try:
                pr = persistence.persist(process_result, SourceTag.DIP_GACETA_TABLA)
                if pr.get("asset_inserted"):
                    stats["assets_insertados"] += 1
                else:
                    stats["assets_skipped"] += 1
            except Exception as exc:
                logger.exception("Error persistiendo tabla INDETERMINATE %s", numero)
                stats["errores"].append(f"tabla {numero} persist INDETERMINATE: {exc}")
            continue

        if process_result.classification == "FAIL":
            logger.warning(
                "Tabla %s FAIL: %s",
                numero,
                process_result.parser_errors,
            )
            stats["errores"].append(
                f"tabla {numero} FAIL: {process_result.parser_errors}"
            )
            try:
                pr = persistence.persist(process_result, SourceTag.DIP_GACETA_TABLA)
                if pr.get("asset_inserted"):
                    stats["assets_insertados"] += 1
                else:
                    stats["assets_skipped"] += 1
            except Exception as exc:
                logger.exception("Error persistiendo tabla FAIL %s", numero)
                stats["errores"].append(f"tabla {numero} persist FAIL: {exc}")
            continue

        # --- SUCCESS ---
        stats["tablas_procesadas"] += 1

        try:
            pr = persistence.persist(process_result, SourceTag.DIP_GACETA_TABLA)
        except Exception as exc:
            logger.exception("Error persistiendo tabla SUCCESS %s", numero)
            stats["errores"].append(f"tabla {numero} persist SUCCESS: {exc}")
            continue

        if pr.get("asset_inserted"):
            stats["assets_insertados"] += 1
        else:
            stats["assets_skipped"] += 1

        if pr.get("event_inserted"):
            stats["vote_events_insertados"] += 1
        else:
            stats["vote_events_existentes"] += 1

        stats["casts_insertados"] += pr.get("casts_inserted", 0)
        stats["counts_insertados"] += pr.get("counts_inserted", 0)

        # Filtrado por fecha
        vote_date = None
        if process_result.vote_event:
            vote_date = process_result.vote_event.get("vote_date")

        skip_posts = False
        if since is not None and vote_date is not None and vote_date < since:
            logger.info(
                "Tabla %s fecha %s anterior a since %s; omitiendo POSTs",
                numero,
                vote_date,
                since,
            )
            skip_posts = True

        if skip_posts:
            continue

        # Extraer lola_mapping de parsed_data["meta"]["lola_mapping"]
        lola_mapping: dict[str, dict] = {}
        if process_result.parsed_data and isinstance(process_result.parsed_data, dict):
            meta = process_result.parsed_data.get("meta", {})
            if isinstance(meta, dict):
                lola_json = meta.get("lola_mapping")
                if isinstance(lola_json, str):
                    try:
                        lola_mapping = json.loads(lola_json)
                    except json.JSONDecodeError as exc:
                        logger.warning(
                            "lola_mapping no parseable en tabla %s: %s",
                            numero,
                            exc,
                        )
                        stats["errores"].append(
                            f"tabla {numero} lola_mapping JSON: {exc}"
                        )

        if not lola_mapping:
            logger.info("Tabla %s sin celdas activas (lola_mapping vacío)", numero)
            continue

        post_url = f"{BASE_URL}lanordi{numero}.php3"

        for lola_name, lola_info in lola_mapping.items():
            if not isinstance(lola_info, dict):
                continue
            if lola_info.get("value", 0) <= 0:
                continue

            payload_str = f"{lola_name}=Seleccion"
            payload_bytes = payload_str.encode("utf-8")

            logger.debug("POST nominal tabla=%s lola=%s", numero, lola_name)
            try:
                post_fetch = engine.fetch(
                    post_url,
                    method="POST",
                    payload=payload_bytes,
                    source_tag="dip_gaceta_post",
                    extra_headers={"Content-Type": "application/x-www-form-urlencoded"},
                )
            except Exception as exc:
                logger.exception("Error de red en POST %s %s", numero, lola_name)
                stats["errores"].append(f"POST {numero} {lola_name} red: {exc}")
                continue

            stats["post_nominales_enviados"] += 1

            if post_fetch.waf_detected:
                logger.warning("WAF detectado en POST %s %s", numero, lola_name)
                stats["waf_detectados"] += 1
                continue

            post_process = pipeline.process(post_fetch, source_tag="dip_gaceta_post")

            if post_process.classification == "INDETERMINATE":
                stats["indeterminates"] += 1
                logger.info("POST %s %s INDETERMINATE", numero, lola_name)

            try:
                post_pr = persistence.persist(post_process, SourceTag.DIP_GACETA_POST)
                if post_pr.get("asset_inserted"):
                    stats["assets_insertados"] += 1
                else:
                    stats["assets_skipped"] += 1
                if post_pr.get("event_inserted"):
                    stats["vote_events_insertados"] += 1
                else:
                    stats["vote_events_existentes"] += 1
                stats["casts_insertados"] += post_pr.get("casts_inserted", 0)
                stats["counts_insertados"] += post_pr.get("counts_inserted", 0)
            except Exception as exc:
                logger.exception("Error persistiendo POST %s %s", numero, lola_name)
                stats["errores"].append(
                    f"POST {numero} {lola_name} persist: {exc}"
                )

    return stats
