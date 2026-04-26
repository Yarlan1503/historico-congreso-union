"""Backward compatible wrapper — see sitl.py for implementation."""

from scraper.sources.sitl import _extract_votacion_ids, scrape_sitl_lxvi

__all__ = ["scrape_sitl_lxvi", "_extract_votacion_ids"]
