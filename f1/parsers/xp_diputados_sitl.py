"""Parser transductor temporal para fuentes Diputados SITL/INFOPAL.

Este módulo expone una única función pública, `parse_response`, que recibe
el cuerpo de bytes de una respuesta HTTP y devuelve bien los conteos
agregados (y lista nominal si existe) o un objeto `Indeterminate` con la
razón del fallo.

El parser es completamente defensivo: nunca lanza excepciones silenciosas
y maneja encoding de forma explícita.
"""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING

from bs4 import BeautifulSoup

from f1.parsers.xp_types import Indeterminate, ParsedCounts, XPCounts, XPIndeterminate, XPVoteCast
from f1.parsers.xp_utils import (
    _decode_body,
    _detect_waf,
    _normalize_sentido,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Mapeo normalizado de claves de conteo agregado (específico de SITL)
# ---------------------------------------------------------------------------
_COUNT_ALIASES: dict[str, str] = {
    "a favor": "a_favor",
    "en contra": "en_contra",
    "abstencion": "abstencion",
    "abstención": "abstencion",
    "ausente": "ausente",
    "ausentes": "ausente",
    "no voto": "novoto",
    "no votó": "novoto",
    "novoto": "novoto",
}


def _normalize_count_key(raw: str) -> str | None:
    """Normaliza una etiqueta de conteo agregado a clave canónica."""
    cleaned = raw.strip().lower()
    return _COUNT_ALIASES.get(cleaned)


def _extract_counts_from_soup(soup: BeautifulSoup) -> XPCounts | None:
    """Extrae conteos agregados del HTML usando heurísticas sobre tablas y texto.

    La estrategia:
    1. Buscar tablas donde encabezados o celdas contengan etiquetas de voto.
    2. Si falla, buscar en texto libre números cercanos a dichas etiquetas.
    3. Si no se encuentra ningún conteo, devuelve ``None``.
    """
    counts: dict[str, int] = {}

    # ------------------------------------------------------------------
    # Estrategia A: tablas con encabezados de conteo
    # ------------------------------------------------------------------
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue
        header_row = rows[0]
        headers: list[str] = []
        for th in header_row.find_all(["th", "td"]):
            headers.append(th.get_text(strip=True))

        # Si los encabezados parecen conteos, buscar fila TOTAL o fallback a primera fila
        canonical_headers = [_normalize_count_key(h) for h in headers]
        if any(ch is not None for ch in canonical_headers):
            total_row = None
            first_data_row = None
            for data_row in rows[1:]:
                if first_data_row is None:
                    first_data_row = data_row
                cells = data_row.find_all(["td", "th"])
                if cells:
                    first_cell_text = cells[0].get_text(strip=True)
                    normalized = first_cell_text.lower().replace("ó", "o").strip()
                    if normalized == "total":
                        total_row = data_row
                        break

            target_row = total_row if total_row is not None else first_data_row
            if target_row is not None:
                cells = target_row.find_all(["td", "th"])
                for idx, ch in enumerate(canonical_headers):
                    if ch is None or idx >= len(cells):
                        continue
                    text_num = cells[idx].get_text(strip=True)
                    try:
                        counts[ch] = int(text_num)
                    except ValueError:
                        logger.debug("No se pudo convertir a int: %s", text_num)
                        continue
            # Si ya tenemos al menos un conteo, preferimos esta tabla
            if counts:
                break

    # ------------------------------------------------------------------
    # Estrategia B: si la tabla no funcionó, regex línea por línea
    # ------------------------------------------------------------------
    if not counts:
        text = soup.get_text(separator="\n", strip=True)
        for alias, key in _COUNT_ALIASES.items():
            if key in counts:
                continue
            pattern = rf"{re.escape(alias)}\s*[\:\-]?\s*(?:\()?\s*(\d+)\s*(?:\))?"
            for line in text.splitlines():
                match = re.search(pattern, line, re.IGNORECASE)
                if match:
                    num_str = match.group(1)
                    try:
                        counts[key] = int(num_str)
                    except ValueError:
                        continue
                    break

    if not counts:
        return None

    return XPCounts(
        a_favor=counts.get("a_favor", 0),
        en_contra=counts.get("en_contra", 0),
        abstencion=counts.get("abstencion", 0),
        ausente=counts.get("ausente", 0),
        novoto=counts.get("novoto", 0),
    )


def _extract_nominal_from_soup(soup: BeautifulSoup, source_tag: str) -> list[XPVoteCast]:
    """Extrae la lista nominal de votos si existe.

    Heurística:
    - Recorre todas las tablas buscando filas con al menos 3 celdas.
    - Intenta inferir qué columna es iddip, partidot y sentido.
    - Requiere que al menos una celda de la fila sea numérica (iddip) y otra
      sea un sentido reconocible.
    """
    nominal: list[XPVoteCast] = []

    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue

        # Determinar si hay encabezados útiles
        header_row = rows[0]
        header_cells = header_row.find_all(["th", "td"])
        header_texts = [h.get_text(strip=True).lower() for h in header_cells]

        # Mapeo por posición basado en encabezados
        pos_iddip: int | None = None
        pos_partido: int | None = None
        pos_sentido: int | None = None

        for idx, htext in enumerate(header_texts):
            if htext in {"iddip", "id", "diputado", "nombre", "legislador"}:
                pos_iddip = idx
            if htext in {"partido", "partidot", "grupo", "fraccion", "fracción"}:
                pos_partido = idx
            if htext in {"voto", "sentido", "posicion", "posición"}:
                pos_sentido = idx

        data_rows = rows[1:] if pos_iddip is not None and pos_sentido is not None else rows

        for tr in data_rows:
            cells = tr.find_all(["td", "th"])
            if len(cells) < 3:
                continue

            # Si no hay encabezado, intentar inferir por contenido
            if pos_iddip is None or pos_sentido is None or pos_partido is None:
                # Buscar una celda numérica como candidata a iddip
                for idx, cell in enumerate(cells):
                    text = cell.get_text(strip=True)
                    if text.isdigit() and pos_iddip is None:
                        pos_iddip = idx
                    sentido_norm = _normalize_sentido(text, source_tag)
                    if sentido_norm is not None and pos_sentido is None:
                        pos_sentido = idx
                    # partido es cualquier otra celda de texto largo
                    if (
                        pos_partido is None
                        and text
                        and not text.isdigit()
                        and sentido_norm is None
                        and len(text) > 2
                    ):
                        pos_partido = idx

            if pos_iddip is None or pos_sentido is None:
                continue

            iddip = cells[pos_iddip].get_text(strip=True)
            sentido_raw = cells[pos_sentido].get_text(strip=True)
            sentido = _normalize_sentido(sentido_raw, source_tag)
            if sentido is None or not iddip.isdigit():
                continue

            partidot = ""
            if pos_partido is not None and pos_partido < len(cells):
                partidot = cells[pos_partido].get_text(strip=True)

            nominal.append(
                XPVoteCast(
                    iddip=iddip,
                    partidot=partidot,
                    sentido=sentido,  # type: ignore[typeddict-item]
                )
            )

        # Si ya extrajimos registros de esta tabla, preferimos no mezclar con otra
        if nominal:
            break

    return nominal


def _build_indeterminate(
    source_tag: str,
    parser_version: str,
    reason: str,
    detail: str,
) -> XPIndeterminate:
    """Fábrica inmutable para objetos ``Indeterminate``."""
    return XPIndeterminate(
        source_tag=source_tag,
        parser_version=parser_version,
        reason=reason,  # type: ignore[typeddict-item]
        detail=detail,
    )


# ---------------------------------------------------------------------------
# API pública
# ---------------------------------------------------------------------------

def parse_response(
    body: bytes,
    source_tag: str,
    parser_version: str,
) -> ParsedCounts | Indeterminate:
    """Parsea el cuerpo de bytes de una respuesta SITL/INFOPAL.

    Args:
        body: Cuerpo de la respuesta HTTP en bytes.
        source_tag: Etiqueta de la fuente; se espera ``"dip_sitl"`` o ``"dip_infopal"``.
        parser_version: Versión semántica del parser, p. ej. ``"dip_sitl_0.1.0"``.

    Returns:
        ``ParsedCounts`` si se extrajeron conteos, o ``Indeterminate`` con la
        razón del fallo.
    """
    try:
        text, used_encoding = _decode_body(body)
    except Exception as exc:  # pragma: no cover
        logger.exception("Decoding failure")
        return _build_indeterminate(
            source_tag=source_tag,
            parser_version=parser_version,
            reason="UNKNOWN",
            detail=f"Error de decodificacion: {exc} (encoding intentado: utf-8 / iso-8859-1)",
        )

    # ------------------------------------------------------------------
    # 1. Detección de bloqueo / WAF / cache / timeout
    # ------------------------------------------------------------------
    block_result = _detect_waf(text)
    if block_result is not None:
        return XPIndeterminate(
            source_tag=source_tag,
            parser_version=parser_version,
            reason=block_result["reason"],  # type: ignore[typeddict-item]
            detail=block_result["detail"],
        )

    # ------------------------------------------------------------------
    # 2. Parseo HTML
    # ------------------------------------------------------------------
    try:
        soup = BeautifulSoup(text, "html.parser")
    except Exception as exc:
        logger.exception("BeautifulSoup parse error")
        return _build_indeterminate(
            source_tag=source_tag,
            parser_version=parser_version,
            reason="UNKNOWN",
            detail=f"HTML parse error: {exc}",
        )

    # ------------------------------------------------------------------
    # 3. Extracción de conteos agregados
    # ------------------------------------------------------------------
    counts = _extract_counts_from_soup(soup)
    if counts is None:
        return _build_indeterminate(
            source_tag=source_tag,
            parser_version=parser_version,
            reason="UNKNOWN",
            detail="estructura no reconocida",
        )

    # ------------------------------------------------------------------
    # 4. Extracción de lista nominal (opcional)
    # ------------------------------------------------------------------
    nominal = _extract_nominal_from_soup(soup, source_tag)

    meta: dict[str, str] = {
        "encoding": used_encoding,
    }

    return ParsedCounts(
        source_tag=source_tag,
        parser_version=parser_version,
        counts=counts,
        nominal=nominal,
        meta=meta,
    )


# ---------------------------------------------------------------------------
# Bloque de demostración
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    # Ejemplo 1: HTML completo con conteos y lista nominal
    html_ok = (
        '<html><body>'
        '<table class="resumen">'
        '<tr><th>A FAVOR</th><th>EN CONTRA</th>'
        '<th>ABSTENCION</th><th>AUSENTE</th><th>NO VOTO</th></tr>'
        '<tr><td>10</td><td>5</td><td>2</td><td>1</td><td>0</td></tr>'
        '</table>'
        '<table class="detalle">'
        '<tr><th>IdDip</th><th>Partido</th><th>Voto</th></tr>'
        '<tr><td>101</td><td>PARTIDO A</td><td>A FAVOR</td></tr>'
        '<tr><td>102</td><td>PARTIDO B</td><td>EN CONTRA</td></tr>'
        '<tr><td>103</td><td>PARTIDO C</td><td>ABSTENCION</td></tr>'
        '</table>'
        '</body></html>'
    )
    result1 = parse_response(
        body=html_ok.encode("utf-8"),
        source_tag="dip_sitl",
        parser_version="dip_sitl_0.1.0",
    )
    print("Ejemplo 1 (OK completo):", result1)

    # Ejemplo 2: Solo conteos agregados, sin lista nominal
    html_counts_only = (
        '<html><body>'
        '<div class="resultados">'
        '<p>A FAVOR: 20</p>'
        '<p>EN CONTRA: 10</p>'
        '<p>ABSTENCION: 3</p>'
        '<p>AUSENTE: 2</p>'
        '<p>NO VOTO: 0</p>'
        '</div>'
        '</body></html>'
    )
    result2 = parse_response(
        body=html_counts_only.encode("utf-8"),
        source_tag="dip_infopal",
        parser_version="dip_sitl_0.1.0",
    )
    print("Ejemplo 2 (solo conteos):", result2)

    # Ejemplo 3: WAF / bloqueo (Incapsula)
    html_waf = (
        '<html><head><title>Incapsula incident ID: 12345</title></head>'
        '<body>Access Denied - Your request has been blocked by Incapsula.</body></html>'
    )
    result3 = parse_response(
        body=html_waf.encode("utf-8"),
        source_tag="dip_sitl",
        parser_version="dip_sitl_0.1.0",
    )
    print("Ejemplo 3 (WAF):", result3)
