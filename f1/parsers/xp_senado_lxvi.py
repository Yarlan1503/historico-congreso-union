"""Parser transductor temporal para fuente Senado LXVI (portal actual).

Soporta dos source_tags:
- ``senado_lxvi_html``: página completa de votación ``/66/votacion/{id}``.
- ``senado_lxvi_ajax``: fragmento AJAX hipotético (HTML parcial o JSON).

El parser es defensivo: nunca lanza excepciones silenciosas y devuelve
``Indeterminate`` cuando detecta WAF, bloqueos o estructuras no reconocidas.
"""

from __future__ import annotations

import json
import logging
import re

from bs4 import BeautifulSoup

from f1.parsers.xp_types import (
    Indeterminate,
    ParsedSenadoLXVI,
    XPCounts,
    XPIndeterminate,
    XPSenatorCast,
)
from f1.parsers.xp_utils import (
    _decode_body,
    _detect_waf,
    _normalize_sentido,
    _validate_counts_vs_nominal,
)

logger = logging.getLogger(__name__)

_COUNT_KEYWORDS: dict[str, tuple[str, ...]] = {
    "a_favor": ("a favor", "favor", "si", "sí"),
    "en_contra": ("en contra", "contra", "no"),
    "abstencion": ("abstencion", "abstención", "abstenciones"),
    "ausente": ("ausente", "ausentes"),
    "quorum": ("quorum", "quórum"),
}


def _extract_counts_from_tfoot(soup: BeautifulSoup) -> XPCounts | None:
    """Extrae conteos agregados desde un ``<tfoot>`` con celdas de resumen.

    Busca la tabla con ``<tfoot>`` y parsea celdas que contengan patrones como
    ``EN PRO``, ``EN CONTRA``, ``ABSTENCIÓN``/``ABSTENCION`` junto con un número.
    """
    tfoot = soup.find("tfoot")
    if not tfoot:
        return None

    counts: dict[str, int] = {
        "a_favor": 0,
        "en_contra": 0,
        "abstencion": 0,
        "ausente": 0,
    }
    found_any = False

    for cell in tfoot.find_all(["td", "th"]):
        cell_text = cell.get_text(separator=" ", strip=True).lower()
        # Extraer el número presente en la celda (puede estar en un <span> o plano)
        num_match = re.search(r"\d+", cell_text)
        if not num_match:
            continue
        num = int(num_match.group())

        if "en pro" in cell_text or ("pro" in cell_text and "contra" not in cell_text):
            counts["a_favor"] = num
            found_any = True
        elif "en contra" in cell_text:
            counts["en_contra"] = num
            found_any = True
        elif "abstención" in cell_text or "abstencion" in cell_text:
            counts["abstencion"] = num
            found_any = True
        elif "ausente" in cell_text:
            counts["ausente"] = num
            found_any = True

    return counts if found_any else None  # type: ignore[return-value]


def _extract_counts_from_text(text: str) -> XPCounts:
    """Extrae conteos agregados buscando patrones tipo ``A favor: 42``."""
    counts: dict[str, int] = {
        "a_favor": 0,
        "en_contra": 0,
        "abstencion": 0,
        "ausente": 0,
    }
    # Patrón general: palabra(s) clave opcionalmente seguidas de ':' o '=' y un número
    # Ejemplo: "A favor: 42", "Abstenciones = 3", "Quorum 80"
    pattern = re.compile(
        r"(?P<label>[A-Za-zÁáÉéÍíÓóÚúñÑ\s]+)"
        r"[:\s=]*"
        r"(?P<num>\d+)",
        re.IGNORECASE,
    )
    for match in pattern.finditer(text):
        label = match.group("label").strip().lower()
        num = int(match.group("num"))
        for key, keywords in _COUNT_KEYWORDS.items():
            if any(kw in label for kw in keywords):
                counts[key] = num
                break
    return counts  # type: ignore[return-value]


def _extract_nominal_from_table(soup: BeautifulSoup, source_tag: str) -> list[XPSenatorCast]:
    """Extrae votos nominales de cualquier ``<table>`` con columnas reconocibles."""
    rows: list[XPSenatorCast] = []
    for table in soup.find_all("table"):
        headers: list[str] = []
        thead = table.find("thead")
        if thead:
            headers = [th.get_text(strip=True).lower() for th in thead.find_all(["th", "td"])]
        else:
            first_row = table.find("tr")
            if first_row:
                headers = [
                    cell.get_text(strip=True).lower()
                    for cell in first_row.find_all(["th", "td"])
                ]
        # Determinar índices de columnas por heurística
        nombre_keys = ("senador", "nombre", "diputado", " legislador")
        idx_nombre = next(
            (i for i, h in enumerate(headers) if any(k in h for k in nombre_keys)),
            None,
        )
        grupo_keys = ("grupo", "partido", "fraccion", "fracción")
        idx_grupo = next(
            (i for i, h in enumerate(headers) if any(k in h for k in grupo_keys)),
            None,
        )
        sentido_keys = ("sentido", "voto", "votación", "posición")
        idx_sentido = next(
            (i for i, h in enumerate(headers) if any(k in h for k in sentido_keys)),
            None,
        )
        if idx_nombre is None and idx_sentido is None:
            # Tabla sin encabezados reconocibles: saltar
            continue

        # Extraer filas de datos, saltando explícitamente thead
        tbody = table.find("tbody")
        if tbody:
            data_rows = tbody.find_all("tr")
        else:
            all_rows = table.find_all("tr")
            thead_rows = set()
            if thead:
                thead_rows = {id(tr) for tr in thead.find_all("tr")}
            data_rows = [tr for tr in all_rows if id(tr) not in thead_rows]
            if not thead and data_rows:
                data_rows = data_rows[1:]  # Saltar fila de headers inferida

        # Heurística: detectar columna índice numérica extra al inicio
        offset = 0
        if data_rows:
            first_data = data_rows[0]
            cells = first_data.find_all(["td", "th"])
            if len(cells) > len(headers) and re.fullmatch(
                r"\d+", cells[0].get_text(strip=True)
            ):
                offset = 1

        for tr in data_rows:
            cells = tr.find_all(["td", "th"])
            if len(cells) < 2:
                continue
            real_idx_nombre = idx_nombre + offset if idx_nombre is not None else None
            real_idx_grupo = idx_grupo + offset if idx_grupo is not None else None
            real_idx_sentido = idx_sentido + offset if idx_sentido is not None else None
            nombre = (
                cells[real_idx_nombre].get_text(strip=True)
                if real_idx_nombre is not None and real_idx_nombre < len(cells)
                else ""
            )
            grupo = (
                cells[real_idx_grupo].get_text(strip=True)
                if real_idx_grupo is not None and real_idx_grupo < len(cells)
                else ""
            )
            sentido_raw = (
                cells[real_idx_sentido].get_text(strip=True)
                if real_idx_sentido is not None and real_idx_sentido < len(cells)
                else ""
            )
            if not nombre and not sentido_raw:
                continue
            sentido = _normalize_sentido(sentido_raw, source_tag)
            if sentido is None:
                sentido = "novoto"
            rows.append(
                {
                    "nombre": nombre,
                    "grupo": grupo,
                    "sentido": sentido,
                }
            )
    return rows


def _sum_counts_from_nominal(nominal: list[XPSenatorCast]) -> XPCounts:
    """Suma sentidos de voto desde la lista nominal a un conteo base."""
    counts: dict[str, int] = {
        "a_favor": 0,
        "en_contra": 0,
        "abstencion": 0,
        "ausente": 0,
    }
    for row in nominal:
        sentido = row.get("sentido", "")
        if sentido == "a_favor":
            counts["a_favor"] += 1
        elif sentido == "en_contra":
            counts["en_contra"] += 1
        elif sentido == "abstencion":
            counts["abstencion"] += 1
        elif sentido == "ausente":
            counts["ausente"] += 1
        elif sentido == "presente":
            counts["presente"] = counts.get("presente", 0) + 1
        elif sentido == "novoto":
            counts["novoto"] = counts.get("novoto", 0) + 1
    return counts  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# Parser específico para HTML completo
# ---------------------------------------------------------------------------
def _parse_html(
    text: str,
    source_tag: str,
    parser_version: str,
) -> ParsedSenadoLXVI | XPIndeterminate:
    """Parsea el HTML completo de una página de votación del Senado."""
    soup = BeautifulSoup(text, "html.parser")

    # --- metadata ----------------------------------------------------------
    metadata: dict[str, str] = {}
    title_tag = soup.find("title")
    if title_tag:
        metadata["titulo"] = title_tag.get_text(strip=True)
    h1 = soup.find("h1")
    if h1:
        metadata["titulo"] = h1.get_text(strip=True)

    # Fecha: priorizar extracción estructurada desde <strong> en div.text-center,
    # que contiene la fecha de la votación (ej. "Martes 24 de octubre de 2006").
    # Los patrones posteriores en la página corresponden a la sidebar "últimas votaciones".
    _MESES = (
        "enero|febrero|marzo|abril|mayo|junio|"
        "julio|agosto|septiembre|octubre|noviembre|diciembre"
    )
    _RE_SPANISH_DATE = re.compile(
        rf"(\d{{1,2}}\s+de\s+(?:{_MESES})\s+de\s+\d{{4}})", re.IGNORECASE
    )
    _RE_NUMERIC_DATE = re.compile(r"\b(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\b")

    fecha_strong = soup.find("strong", string=_RE_SPANISH_DATE)
    if fecha_strong:
        # Extracción estructurada: fecha dentro de <strong> en div.text-center
        metadata["fecha"] = fecha_strong.get_text(strip=True)
    else:
        # Fallback 1: primer match de fecha en formato español en el texto
        fecha_match = _RE_SPANISH_DATE.search(text)
        if fecha_match:
            metadata["fecha"] = fecha_match.group(1)
        else:
            # Fallback 2: fecha numérica DD/MM/YYYY (formato original del parser)
            fecha_match = _RE_NUMERIC_DATE.search(text)
            if fecha_match:
                metadata["fecha"] = fecha_match.group(1)

    # Tipo de votación por heurística de palabras clave
    lowered_text = text.lower()
    if "economica" in lowered_text or "económica" in lowered_text:
        metadata["tipo"] = "Economica"
    elif "ordinaria" in lowered_text:
        metadata["tipo"] = "Ordinaria"
    elif "extraordinaria" in lowered_text:
        metadata["tipo"] = "Extraordinaria"

    # Resultado
    if "aprobada" in lowered_text:
        metadata["resultado"] = "Aprobada"
    elif "rechazada" in lowered_text:
        metadata["resultado"] = "Rechazada"

    # --- conteos -----------------------------------------------------------
    counts = _extract_counts_from_tfoot(soup)
    if counts is None:
        counts = _extract_counts_from_text(text)

    # --- tabla nominal -----------------------------------------------------
    nominal = _extract_nominal_from_table(soup, source_tag)

    # --- validación --------------------------------------------------------
    if counts["a_favor"] == 0 and counts["en_contra"] == 0 and not nominal:
        # Si no hay <tfoot> ni tabla nominal, probablemente es página índice;
        # devolver counts limpios en lugar de valores espurios o Indeterminate.
        has_tfoot = soup.find("tfoot") is not None
        if not has_tfoot:
            counts = {
                "a_favor": 0,
                "en_contra": 0,
                "abstencion": 0,
                "ausente": 0,
            }
        else:
            return Indeterminate(
                reason="UNKNOWN",
                detail="estructura no reconocida",
            )

    return {
        "source_tag": source_tag,
        "parser_version": parser_version,
        "metadata": metadata,
        "counts": counts,
        "nominal": nominal,
    }


# ---------------------------------------------------------------------------
# Parser específico para AJAX
# ---------------------------------------------------------------------------
def _parse_ajax(
    text: str,
    source_tag: str,
    parser_version: str,
) -> ParsedSenadoLXVI | XPIndeterminate:
    """Parsea la respuesta AJAX hipotética (JSON o HTML parcial)."""
    # 1. Intentar JSON
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        payload = None

    if isinstance(payload, dict):
        # Heurística: buscar listas de diccionarios con claves similares a nominal
        nominal: list[XPSenatorCast] = []
        for _key, value in payload.items():
            if isinstance(value, list) and value and isinstance(value[0], dict):
                for item in value:
                    if not isinstance(item, dict):
                        continue
                    nombre = str(
                        item.get("nombre", item.get("senador", item.get("diputado", "")))
                    ).strip()
                    grupo = str(
                        item.get("grupo", item.get("partido", item.get("fraccion", "")))
                    ).strip()
                    sentido_raw = str(item.get("sentido", item.get("voto", ""))).strip()
                    sentido = _normalize_sentido(sentido_raw, source_tag)
                    if sentido is None:
                        sentido = "novoto"
                    if nombre or sentido_raw:
                        nominal.append(
                            {
                                "nombre": nombre,
                                "grupo": grupo,
                                "sentido": sentido,
                            }
                        )
        if nominal:
            return {
                "source_tag": source_tag,
                "parser_version": parser_version,
                "metadata": {},
                "counts": _sum_counts_from_nominal(nominal),
                "nominal": nominal,
            }

    # 2. Intentar HTML parcial (tabla)
    soup = BeautifulSoup(text, "html.parser")
    nominal_html = _extract_nominal_from_table(soup, source_tag)
    if nominal_html:
        return {
            "source_tag": source_tag,
            "parser_version": parser_version,
            "metadata": {},
            "counts": _sum_counts_from_nominal(nominal_html),
            "nominal": nominal_html,
        }

    return Indeterminate(
        reason="UNKNOWN",
        detail="AJAX body no reconocido",
    )


# ---------------------------------------------------------------------------
# Entrypoint público
# ---------------------------------------------------------------------------
def parse_response(
    body: bytes,
    source_tag: str,
    parser_version: str,
) -> ParsedSenadoLXVI | XPIndeterminate:
    """Parsea la respuesta cruda del Senado LXVI.

    Args:
        body: Cuerpo de la respuesta HTTP en bytes.
        source_tag: Identificador de la fuente. Valores esperados:
            ``senado_lxvi_html`` o ``senado_lxvi_ajax``.
        parser_version: Versión del parser (ej. ``"senado_lxvi_0.1.0"``).

    Returns:
        ``ParsedSenadoLXVI`` si la extracción tiene éxito,
        ``Indeterminate`` en caso contrario.
    """
    text, _ = _decode_body(body)

    waf = _detect_waf(text)
    if waf is not None:
        return XPIndeterminate(
            source_tag=source_tag,
            parser_version=parser_version,
            reason=waf["reason"],
            detail=waf["detail"],
        )

    if source_tag in ("senado_lxvi_html", "sen_lxvi_html"):
        result = _parse_html(text, source_tag, parser_version)
    elif source_tag in ("senado_lxvi_ajax", "sen_lxvi_ajax"):
        result = _parse_ajax(text, source_tag, parser_version)
    else:
        return Indeterminate(
            reason="UNKNOWN",
            detail=f"source_tag no soportado: {source_tag}",
        )

    if isinstance(result, dict) and "counts" in result and "nominal" in result:
        validation = _validate_counts_vs_nominal(result["counts"], result["nominal"])
        if not validation["ok"]:
            logger.warning("Counts vs nominal mismatch: %s", validation)
        result.setdefault("meta", {})
        result["meta"]["validation_counts_vs_nominal"] = json.dumps(validation)

    return result


# ---------------------------------------------------------------------------
# Ejemplos mínimos
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # Ejemplo 1: HTML completo (simulado)
    html_sample = b"""<!DOCTYPE html>
<html><head><title>Votaci\xc3\xb3n 123 - Senado</title></head>
<body>
<h1>Votaci\xc3\xb3n 123</h1>
<p>Fecha: 15/03/2024</p>
<p>A favor: 62</p>
<p>En contra: 3</p>
<p>Abstenciones: 5</p>
<table>
<tr><th>Senador</th><th>Grupo</th><th>Sentido</th></tr>
<tr><td>Juan P\xc3\xa9rez</td><td>Grupo A</td><td>A favor</td></tr>
<tr><td>Mar\xc3\xada L\xc3\xb3pez</td><td>Grupo B</td><td>En contra</td></tr>
</table>
</body></html>
"""
    result = parse_response(html_sample, "senado_lxvi_html", "senado_lxvi_0.1.0")
    print("HTML sample:", result)

    # Ejemplo 2: AJAX JSON
    json_sample = (
        b'{"votos": [{"nombre": "Ana G\xc3\xb3mez", '
        b'"grupo": "Grupo C", "sentido": "Abstenci\xc3\xb3n"}]}'
    )
    result2 = parse_response(json_sample, "senado_lxvi_ajax", "senado_lxvi_0.1.0")
    print("JSON sample:", result2)

    # Ejemplo 3: WAF
    waf_sample = b"<html><body>Incapsula incident ID: 12345</body></html>"
    result3 = parse_response(waf_sample, "senado_lxvi_html", "senado_lxvi_0.1.0")
    print("WAF sample:", result3)

    # Ejemplo 4: AJAX no reconocido
    bad_ajax = b"some random text"
    result4 = parse_response(bad_ajax, "senado_lxvi_ajax", "senado_lxvi_0.1.0")
    print("Bad AJAX:", result4)
