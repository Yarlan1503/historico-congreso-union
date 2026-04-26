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
    assert isinstance(ids, dict)
    assert sorted(ids.keys()) == ["123", "456", "999"]


def test_extract_votacion_ids_with_dates():
    """Extrae IDs con sus fechas asociadas desde HTML con estructura de tabla."""
    html = (
        b"<html><body><table>"
        b"<tr><td>3 Septiembre 2024</td></tr>"
        b'<tr><td><a href="estadistico_votacionnplxvi.php?votaciont=2">'
        b"1</a></td><td>Some title</td></tr>"
        b'<tr><td><a href="estadistico_votacionnplxvi.php?votaciont=3">'
        b"2</a></td><td>Another title</td></tr>"
        b"<tr><td>18 Septiembre 2024</td></tr>"
        b'<tr><td><a href="estadistico_votacionnplxvi.php?votaciont=5">'
        b"3</a></td><td>Third title</td></tr>"
        b"</table></body></html>"
    )
    result = _extract_votacion_ids(html)
    assert isinstance(result, dict)
    assert result["2"] == "3 Septiembre 2024"
    assert result["3"] == "3 Septiembre 2024"
    assert result["5"] == "18 Septiembre 2024"


def test_extract_votacion_ids_no_dates():
    """IDs sin filas de fecha previa reciben None como fecha."""
    html = (
        b"<html><body><table>"
        b'<tr><td><a href="estadistico_votacionnplxvi.php?votaciont=123">1</a></td></tr>'
        b"</table></body></html>"
    )
    result = _extract_votacion_ids(html)
    assert result == {"123": None}


def test_extract_votacion_ids_merged_rows_ignored():
    """Filas 'merged' con muchas celdas no se interpretan como date rows."""
    # 5 celdas > 3 → no es date row aunque contenga texto de fecha
    cells = "".join(f"<td>cell{i}</td>" for i in range(5))
    html = (
        b"<html><body><table>"
        b"<tr>" + cells.encode() + b"</tr>"
        b'<tr><td><a href="estadistico_votacionnplxvi.php?votaciont=10">link</a></td></tr>'
        b"</table></body></html>"
    )
    result = _extract_votacion_ids(html)
    assert result == {"10": None}
