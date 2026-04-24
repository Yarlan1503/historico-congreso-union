"""Tests para scraper/sources/sitl_lxvi.py."""

from scraper.sources.sitl_lxvi import _extract_votacion_ids


def test_extract_votacion_ids():
    """Extrae IDs únicos y ordenados desde HTML de índice de votaciones."""
    html = (
        b"<html><body>"
        b'<a href="estadistico_votacionnplxvi.php?votaciont=123">Votaci\xc3\xb3n 123</a>'
        b'<a href="listados_votacionesnplxvi.php?partidot=1&votaciont=456">Lista 456</a>'
        b'<a href="estadistico_votacionnplxvi.php?votaciont=123">Duplicado 123</a>'
        b'<a href="otro.php?votaciont=999">Otro link</a>'
        b"</body></html>"
    )

    ids = _extract_votacion_ids(html)
    assert ids == ["123", "456", "999"]
