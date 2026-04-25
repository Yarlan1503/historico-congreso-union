"""Scraper para Senado histórico (LXII-LXV).

Portal HTML estático — scrapea índices y páginas de detalle.
Nota: LXIII-LXV están WAF-blocked (Incapsula). LXII tiene índice accesible.

El scraper captura lo que puede (índice + conteos si hay) y marca como
INDETERMINATE lo bloqueado. Diseñado para ser idempotente y defensivo.
"""

from __future__ import annotations

import logging
import tomllib
from datetime import date
from pathlib import Path

from scraper import pipeline

logger = logging.getLogger(__name__)

_CONFIG_PATH = Path(__file__).resolve().parent.parent / "config.toml"


def _load_config() -> dict:
    """Carga la config TOML del scraper."""
    with open(_CONFIG_PATH, "rb") as fh:
        return tomllib.load(fh)


def scrape_senado_historico(
    engine,
    persistence,
    legislature: str = "LXII",
    since: date | None = None,
) -> dict:
    """Scrapea votaciones del Senado histórico.

    Args:
        engine: Motor HTTP (``HTTPScraperEngine``).
        persistence: Capa de persistencia (``ScraperPersistence``).
        legislature: Legislatura (LXII, LXIII, LXIV, LXV).
        since: Filtro de fecha mínima (no implementado para histórico,
            reservado para uso futuro).

    Returns:
        Dict con estadísticas de la ejecución.
    """
    config = _load_config()
    leg_config = config.get("legislatures", {}).get(legislature, {})
    urls = leg_config.get("urls", {}).get("senado", {})

    historico_url = urls.get("historico", "")
    if not historico_url:
        return {
            "source": f"senado_historico_{legislature.lower()}",
            "error_fatal": f"Sin URL histórica para {legislature}",
        }

    source_tag = f"senado_historico_{legislature.lower()}"

    stats = {
        "source": source_tag,
        "legislature": legislature,
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

    # Fetch índice
    logger.info("Scraping Senado histórico %s: %s", legislature, historico_url)
    try:
        index_fetch = engine.fetch(historico_url, method="GET", source_tag=source_tag)
    except Exception as exc:
        logger.exception("Error fetch índice Senado histórico %s", legislature)
        stats["errores"].append(str(exc))
        return stats

    if index_fetch.waf_detected:
        logger.warning("WAF detectado en Senado histórico %s", legislature)
        stats["waf_detectados"] += 1
        try:
            result = pipeline.process(index_fetch, source_tag=source_tag)
            pr = persistence.persist(result, source_tag)
            if pr.get("asset_inserted"):
                stats["assets_insertados"] += 1
            else:
                stats["assets_skipped"] += 1
        except Exception:
            pass
        return stats

    # Procesar respuesta del índice
    result = pipeline.process(index_fetch, source_tag=source_tag)

    try:
        pr = persistence.persist(result, source_tag)
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
    except Exception as exc:
        logger.exception("Error persistiendo índice Senado histórico %s", legislature)
        stats["errores"].append(str(exc))

    if result.classification == "INDETERMINATE":
        stats["indeterminates"] += 1

    return stats
