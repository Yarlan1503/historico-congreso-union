"""Backward compatible wrapper — see senado.py for implementation."""

from __future__ import annotations

from datetime import date

from scraper.sources.senado import scrape_senado


def scrape_senado_lxvi(
    engine,
    persistence,
    since: date | None = None,
    id_range: range | None = None,
    break_on_consecutive_not_found: int = 20,
) -> dict:
    """Backward compatible wrapper — delegates to ``scrape_senado(legislature='LXVI')``."""
    return scrape_senado(
        engine,
        persistence,
        legislature="LXVI",
        since=since,
        id_range=id_range,
        break_on_consecutive_not_found=break_on_consecutive_not_found,
    )


__all__ = ["scrape_senado_lxvi"]
