"""Parser transductor temporal para Gaceta Parlamentaria y POST de Diputados."""

from __future__ import annotations

import json
import re
from typing import Any

from bs4 import BeautifulSoup

try:
    from .xp_types import XPCounts, XPIndeterminate, XPParsedGaceta
    from .xp_utils import _SENTIDO_GLOBAL_MAP, _decode_body, _detect_waf
except ImportError:
    from xp_types import XPCounts, XPIndeterminate, XPParsedGaceta
    from xp_utils import _SENTIDO_GLOBAL_MAP, _decode_body, _detect_waf

ParsedGaceta = XPParsedGaceta
Indeterminate = XPIndeterminate

# Regex for dates
_DATE_RE = re.compile(
    r"\b(?:\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{4}[/-]\d{1,2}[/-]\d{1,2})\b"
)

# Canonical sentido keys used by Gaceta (quorum is not a valid sentido)
_SENTIDO_CANONICAL_KEYS = ("a_favor", "en_contra", "abstencion", "ausente")

_GRUPO_KEYWORDS = ["grupo", "partido", "fracción", "fraccion", "bancada"]

# Known group abbreviations seen in Gaceta vote tables
_GRUPO_ABBREVIATIONS = {
    "mrn", "pan", "pri", "pvem", "pt", "mc", "prd", "sp", "ind", "total"
}


def _looks_like_json(text: str) -> bool:
    """Heurística rápida para determinar si el texto parece JSON."""
    stripped = text.strip()
    return stripped.startswith(("{", "["))


def _extract_metadata_html(soup: BeautifulSoup) -> dict[str, str]:
    """Extrae metadatos (asunto, fecha, etc.) de un documento HTML."""
    metadata: dict[str, str] = {}

    title_tag = soup.find("title")
    if title_tag and title_tag.string:
        metadata["title"] = title_tag.string.strip()

    headers = []
    for tag in soup.find_all(["h1", "h2", "h3"]):
        text = tag.get_text(strip=True)
        if text:
            headers.append(text)
    if headers:
        metadata["asunto"] = headers[0]
        if len(headers) > 1:
            metadata["subtitulo"] = headers[1]

    full_text = soup.get_text(separator=" ", strip=True)
    dates = _DATE_RE.findall(full_text)
    if dates:
        metadata["fecha"] = dates[0]

    label_re = re.compile(r"(?i)asunto|dictamen|fecha\s+de\s+publicación")
    for elem in soup.find_all(string=label_re):
        parent = elem.parent
        if parent is None:
            continue
        text = parent.get_text(strip=True)
        if ":" in text:
            key, val = text.split(":", 1)
            key_norm = (
                key.strip().lower().replace(" ", "_").replace("ó", "o")
            )
            metadata[key_norm] = val.strip()

    return metadata


def _normalize_sentido_header(text: str) -> str | None:
    """Mapea el texto de un encabezado de tabla a una clave de sentido."""
    lower = text.lower()
    for raw, canonical in _SENTIDO_GLOBAL_MAP.items():
        if canonical not in _SENTIDO_CANONICAL_KEYS:
            continue
        # Skip short ambiguous raw values not used by Gaceta
        if raw in ("no", "si", "sí", "pro"):
            continue
        if raw in lower:
            return canonical
    return None


def _is_grupo_header(text: str) -> bool:
    """Determina si un encabezado de tabla corresponde a la columna grupo."""
    lower = text.lower()
    return any(kw in lower for kw in _GRUPO_KEYWORDS)


def _normalize_sentido_row(text: str) -> str | None:
    """Mapea el texto de una fila (primera columna) a una clave de sentido."""
    cleaned = text.strip().lower().replace("ó", "o")
    canonical = _SENTIDO_GLOBAL_MAP.get(cleaned)
    if canonical in _SENTIDO_CANONICAL_KEYS:
        return canonical
    return None


def _looks_like_grupo_header(text: str) -> bool:
    """Heurística para detectar si un header es un nombre de grupo parlamentario."""
    lower = text.lower().strip()
    if not lower or lower in ("votos",):
        return False
    if _is_grupo_header(lower):
        return True
    # Abbreviations like MRN, PAN, PRI, PVEM, PT, MC, PRD, SP, IND
    if lower in _GRUPO_ABBREVIATIONS:
        return True
    # Single-word capitalized abbreviations (2-5 chars)
    return bool(2 <= len(lower) <= 5 and lower.isalpha())


def _extract_table_transposed(soup: BeautifulSoup) -> dict[str, XPCounts]:
    """Busca tablas 'transpuestas' de la Gaceta: filas=sentido, columnas=grupo.

    Estructura típica (con posible fila de título al inicio):
        | <colspan=N> Título del dictamen ... </td>           |
        | Votos | Total | MRN | PAN | ... |
        | Favor | 350   | 244 | 0   | ... |
        | Contra| 111   | 0   | 58  | ... |
        ...
    """
    result: dict[str, XPCounts] = {}

    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 3:
            continue

        # Find the header row: should have multiple cells and first col ~ "Votos"
        header_row_idx = -1
        headers: list[str] = []
        for idx, row in enumerate(rows[:4]):  # Check first few rows
            cells = row.find_all(["th", "td"])
            texts = [c.get_text(strip=True) for c in cells]
            if len(texts) >= 3 and texts[0].lower() in ("votos", "voto"):
                header_row_idx = idx
                headers = texts
                break

        if header_row_idx == -1 or not headers:
            continue

        # Build column map for group names (skip first column)
        col_groups: dict[int, str] = {}
        for idx, h in enumerate(headers[1:], start=1):
            if _looks_like_grupo_header(h):
                col_groups[idx] = h.strip()

        if len(col_groups) < 2:
            continue

        # Accumulate counts per group
        group_counts: dict[str, dict[str, int]] = {
            g: {k: 0 for k in _SENTIDO_CANONICAL_KEYS} for g in col_groups.values()
        }

        for row in rows[header_row_idx + 1 :]:
            cells = row.find_all(["td", "th"])
            if len(cells) < 2:
                continue

            sentido = _normalize_sentido_row(cells[0].get_text(strip=True))
            if sentido is None:
                # Could be "Total" row — skip or use for validation
                continue

            for idx, cell in enumerate(cells):
                if idx not in col_groups:
                    continue
                grupo = col_groups[idx]
                text = cell.get_text(strip=True).replace(",", "")
                # Handle input buttons/values
                if not text:
                    inputs = cell.find_all("input")
                    for inp in inputs:
                        val = inp.get("value", "").strip()
                        if val and val != "0":
                            text = val
                            break
                try:
                    group_counts[grupo][sentido] = int(text)
                except ValueError:
                    nums = re.findall(r"\d+", text)
                    if nums:
                        group_counts[grupo][sentido] = int(nums[0])

        # Convert to XPCounts, excluding empty groups
        for grupo, counts in group_counts.items():
            if any(counts.values()):
                result[grupo] = XPCounts(
                    a_favor=counts["a_favor"],
                    en_contra=counts["en_contra"],
                    abstencion=counts["abstencion"],
                    ausente=counts["ausente"],
                )

        if result:
            break

    return result


def _extract_table_group_sentido(soup: BeautifulSoup) -> dict[str, XPCounts]:
    """Busca y parsea tablas de votación grupo x sentido."""
    result: dict[str, XPCounts] = {}

    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if not rows:
            continue

        header_row = rows[0]
        headers = [
            th.get_text(strip=True) for th in header_row.find_all(["th", "td"])
        ]
        if not headers:
            continue

        col_map: dict[int, str] = {}
        has_grupo = False
        for idx, h in enumerate(headers):
            if _is_grupo_header(h):
                col_map[idx] = "grupo"
                has_grupo = True
            else:
                sentido = _normalize_sentido_header(h)
                if sentido:
                    col_map[idx] = sentido

        if not has_grupo or sum(1 for v in col_map.values() if v != "grupo") < 2:
            continue

        for row in rows[1:]:
            cells = row.find_all(["td", "th"])
            if not cells:
                continue

            grupo_name = None
            counts: dict[str, int] = {k: 0 for k in _SENTIDO_CANONICAL_KEYS}

            for idx, cell in enumerate(cells):
                if idx not in col_map:
                    continue
                kind = col_map[idx]
                text = cell.get_text(strip=True).replace(",", "")
                if kind == "grupo":
                    grupo_name = text
                else:
                    try:
                        counts[kind] = int(text)
                    except ValueError:
                        subs = cell.find_all(["br", "p"])
                        if subs:
                            counts[kind] = len(subs)
                        elif text:
                            nums = re.findall(r"\d+", text)
                            if nums:
                                counts[kind] = int(nums[0])

            if grupo_name:
                result[grupo_name] = XPCounts(
                    a_favor=counts["a_favor"],
                    en_contra=counts["en_contra"],
                    abstencion=counts["abstencion"],
                    ausente=counts["ausente"],
                )

        if result:
            break

    # Fallback: try transposed layout (Gaceta style)
    if not result:
        result = _extract_table_transposed(soup)

    return result


def _extract_json_group_sentido(data: Any) -> dict[str, XPCounts]:
    """Extrae conteos por grupo desde una estructura JSON."""
    result: dict[str, XPCounts] = {}
    if not isinstance(data, dict):
        return result

    vote_keys = ("votos", "votaciones", "grupos", "resultados", "partidos", "bancadas")
    for vk in vote_keys:
        votos = data.get(vk)
        if isinstance(votos, dict):
            for grupo, val in votos.items():
                if isinstance(val, dict):
                    result[grupo] = XPCounts(
                        a_favor=int(val.get("a_favor", val.get("afavor", 0))),
                        en_contra=int(
                            val.get(
                                "en_contra",
                                val.get("encontra", val.get("contra", 0)),
                            )
                        ),
                        abstencion=int(
                            val.get("abstencion", val.get("abstención", 0))
                        ),
                        ausente=int(val.get("ausente", val.get("ausencia", 0))),
                    )
                elif isinstance(val, list):
                    counts = {k: 0 for k in _SENTIDO_CANONICAL_KEYS}
                    for item in val:
                        if isinstance(item, dict):
                            s = str(item.get("sentido", "")).lower()
                        elif isinstance(item, str):
                            s = item.lower()
                        else:
                            continue
                        canonical = _SENTIDO_GLOBAL_MAP.get(s.replace("ó", "o"))
                        if canonical in _SENTIDO_CANONICAL_KEYS:
                            counts[canonical] += 1
                    if any(counts.values()):
                        result[grupo] = XPCounts(
                            a_favor=counts["a_favor"],
                            en_contra=counts["en_contra"],
                            abstencion=counts["abstencion"],
                            ausente=counts["ausente"],
                        )
            if result:
                break
        elif isinstance(votos, list):
            group_map: dict[str, dict[str, int]] = {}
            for item in votos:
                if not isinstance(item, dict):
                    continue
                grupo = str(
                    item.get(
                        "grupo",
                        item.get("partido", item.get("bancada", "Desconocido")),
                    )
                )
                sentido_raw = (
                    str(item.get("sentido", ""))
                    .lower()
                    .replace("ó", "o")
                )
                sentido = _SENTIDO_GLOBAL_MAP.get(sentido_raw)
                if sentido not in _SENTIDO_CANONICAL_KEYS:
                    continue
                if grupo not in group_map:
                    group_map[grupo] = {k: 0 for k in _SENTIDO_CANONICAL_KEYS}
                group_map[grupo][sentido] += 1
            for grupo, counts in group_map.items():
                result[grupo] = XPCounts(
                    a_favor=counts["a_favor"],
                    en_contra=counts["en_contra"],
                    abstencion=counts["abstencion"],
                    ausente=counts["ausente"],
                )
            if result:
                break

    return result


def _extract_json_metadata(data: Any) -> dict[str, str]:
    """Extrae metadatos desde un diccionario JSON."""
    metadata: dict[str, str] = {}
    if not isinstance(data, dict):
        return metadata

    for key in (
        "asunto",
        "dictamen",
        "titulo",
        "fecha",
        "url_gaceta",
        "url",
        "publicacion",
    ):
        val = data.get(key)
        if isinstance(val, str):
            metadata[key] = val
        elif isinstance(val, (list, dict)):
            metadata[key] = json.dumps(val, ensure_ascii=False)

    nested = data.get("metadata") or data.get("meta")
    if isinstance(nested, dict):
        for k, v in nested.items():
            if isinstance(v, str):
                metadata[k] = v
            else:
                metadata[k] = json.dumps(v, ensure_ascii=False)

    return metadata


def _detect_post_evidence_html(
    soup: BeautifulSoup,
    has_metadata: bool,
    has_vote_table: bool,
) -> str | None:
    """Heurísticas para detectar si un HTML parece respuesta de POST.

    Args:
        soup: BeautifulSoup parse tree.
        has_metadata: Whether metadata was extracted from the page.
        has_vote_table: Whether a vote table was found.
    """
    # 1. Interactive vote table with lola submit buttons
    lola_inputs = [
        inp for inp in soup.find_all("input")
        if inp.get("name", "").startswith("lola[")
    ]
    if lola_inputs:
        return "interactive_vote_table_lola_buttons"

    # 2. Form pointing to lanordi* (intermediate vote page)
    for form in soup.find_all("form"):
        action = form.get("action", "")
        if "lanordi" in action.lower():
            return "vote_list_form_lanordi"
        if form.get("method", "").upper() == "POST":
            return "form_post"

    # 3. Hidden inputs typical of POST flows
    for inp in soup.find_all("input"):
        if inp.get("name", "").lower() in ("_method", "csrf", "csrf_token"):
            return "post_form_inputs"

    # 4. Dynamic structure markers
    dynamic_ids = {"tabla-resultados", "resultados", "grid", "ajax-content", "dynamic"}
    dynamic_classes = {"ajax", "dynamic", "generated", "react", "vue", "angular"}

    for elem in soup.find_all(id=True):
        if elem.get("id", "").lower() in dynamic_ids:
            return "dynamic_structure"

    for elem in soup.find_all(class_=True):
        classes = {str(c).lower() for c in elem.get("class", [])}
        if classes & dynamic_classes:
            return "dynamic_structure"

    scripts = " ".join(
        script.string or "" for script in soup.find_all("script") if script.string
    )
    if "POST" in scripts or "$.ajax" in scripts or "fetch(" in scripts:
        return "script_post_reference"

    # 5. Page has metadata but no vote table -> likely intermediate / summary page
    if has_metadata and not has_vote_table:
        # Check if it looks like a gaceta page (footer text)
        full_text = soup.get_text(separator=" ", strip=True).lower()
        if "gaceta parlamentaria" in full_text:
            return "metadata_only_no_vote_table"
        return "metadata_only"

    return None


def _extract_vote_form_meta(soup: BeautifulSoup) -> dict[str, str]:
    """Extrae metadatos de un formulario de votación si existe."""
    meta: dict[str, str] = {}
    for form in soup.find_all("form"):
        action = form.get("action", "").strip()
        if "lanordi" in action.lower() or "voto" in action.lower():
            meta["vote_list_form_action"] = action
            for inp in form.find_all("input", {"type": "hidden"}):
                name = inp.get("name")
                value = inp.get("value", "")
                if name:
                    meta[f"vote_form_{name}"] = value
            break
    return meta


def parse_tabla_agregada(
    body: bytes,
    source_tag: str,
    parser_version: str,
) -> ParsedGaceta | Indeterminate:
    """Parsea una tabla agregada grupo×sentido con inputs ``lola[YY]``.

    Estructura esperada (tabla transpuesta):
        | Votos | Total | MRN | PAN | ... |
        | Favor | 350   | 244 | 0   | ... |
        | Contra| 111   | 0   | 58  | ... |
        ...
    Las celdas activas contienen ``<input type="submit" name="lola[YY]" value="N">``.
    """
    text, _ = _decode_body(body)

    waf = _detect_waf(text)
    if waf is not None:
        return Indeterminate(
            source_tag=source_tag,
            parser_version=parser_version,
            reason=waf["reason"],
            detail=waf["detail"],
        )

    soup = BeautifulSoup(text, "html.parser")

    # Find the first table that contains lola inputs
    lola_re = re.compile(r"^lola\[\d+\]$")
    target_table = None
    for table in soup.find_all("table"):
        if table.find("input", {"name": lola_re}):
            target_table = table
            break

    if target_table is None:
        return Indeterminate(
            reason="UNKNOWN",
            detail="tabla agregada no reconocida",
        )

    rows = target_table.find_all("tr")
    if len(rows) < 3:
        return Indeterminate(
            reason="UNKNOWN",
            detail="tabla agregada no reconocida",
        )

    # Identify header row
    header_row_idx = -1
    headers: list[str] = []
    for idx, row in enumerate(rows):
        cells = row.find_all(["th", "td"])
        texts = [c.get_text(strip=True) for c in cells]
        if len(texts) >= 3 and any(
            _looks_like_grupo_header(t) or t.lower() == "total" for t in texts[1:]
        ):
            header_row_idx = idx
            headers = texts
            break

    if header_row_idx == -1:
        return Indeterminate(
            reason="UNKNOWN",
            detail="tabla agregada no reconocida",
        )

    col_groups: dict[int, str] = {}
    for idx, h in enumerate(headers):
        if idx == 0:
            continue
        if _looks_like_grupo_header(h) or h.lower() == "total":
            col_groups[idx] = h.strip()

    if len(col_groups) < 2:
        return Indeterminate(
            reason="UNKNOWN",
            detail="tabla agregada no reconocida",
        )

    group_counts: dict[str, dict[str, int]] = {
        g: {"a_favor": 0, "en_contra": 0, "abstencion": 0, "ausente": 0}
        for g in col_groups.values()
    }
    lola_mapping: dict[str, dict[str, Any]] = {}

    for row in rows[header_row_idx + 1 :]:
        cells = row.find_all(["td", "th"])
        if len(cells) < 2:
            continue

        sentido = _normalize_sentido_row(cells[0].get_text(strip=True))
        if sentido is None:
            continue

        for idx, cell in enumerate(cells):
            if idx not in col_groups:
                continue
            grupo = col_groups[idx]

            lola_inputs = cell.find_all("input", {"name": lola_re})
            if lola_inputs:
                for inp in lola_inputs:
                    name = str(inp.get("name", "")).strip()
                    value_str = str(inp.get("value", "")).strip()
                    try:
                        value = int(value_str)
                    except ValueError:
                        value = 0
                    if name:
                        lola_mapping[name] = {
                            "grupo": grupo,
                            "sentido": sentido,
                            "value": value,
                        }
                    if sentido in group_counts[grupo]:
                        group_counts[grupo][sentido] = value
            else:
                # Fallback: plain text or button value
                text_val = cell.get_text(strip=True).replace(",", "")
                if not text_val:
                    for inp in cell.find_all("input"):
                        val = str(inp.get("value", "")).strip()
                        if val:
                            text_val = val
                            break
                try:
                    num = int(text_val)
                except ValueError:
                    nums = re.findall(r"\d+", text_val)
                    num = int(nums[0]) if nums else 0
                if sentido in group_counts[grupo]:
                    group_counts[grupo][sentido] = num

    if not any(any(v.values()) for v in group_counts.values()):
        return Indeterminate(
            reason="UNKNOWN",
            detail="tabla agregada no reconocida",
        )

    group_sentido: dict[str, XPCounts] = {}
    for grupo, counts in group_counts.items():
        group_sentido[grupo] = XPCounts(
            a_favor=counts["a_favor"],
            en_contra=counts["en_contra"],
            abstencion=counts["abstencion"],
            ausente=counts["ausente"],
        )

    meta: dict[str, str] = {}
    if lola_mapping:
        meta["lola_mapping"] = json.dumps(lola_mapping, ensure_ascii=False)

    return {
        "source_tag": source_tag,
        "parser_version": parser_version,
        "metadata": {},
        "group_sentido": group_sentido,
        "meta": meta,
    }


def parse_response(
    body: bytes,
    source_tag: str,
    parser_version: str,
) -> ParsedGaceta | Indeterminate:
    """Parsea una respuesta de Gaceta Parlamentaria o POST de Diputados.

    Args:
        body: bytes crudos de la respuesta.
        source_tag: etiqueta de fuente, esperado ``dip_gaceta`` o ``dip_post``.
        parser_version: versión del parser, ej. ``dip_gaceta_0.1.0``.

    Returns:
        XPParsedGaceta con los datos extraídos, o XPIndeterminate si no es posible.
    """
    text, _ = _decode_body(body)

    waf = _detect_waf(text)
    if waf is not None:
        return Indeterminate(
            source_tag=source_tag,
            parser_version=parser_version,
            reason=waf["reason"],
            detail=waf["detail"],
        )

    metadata: dict[str, str] = {}
    group_sentido: dict[str, XPCounts] = {}
    post_evidence: str | None = None
    meta: dict[str, str] = {}

    if _looks_like_json(text):
        try:
            data = json.loads(text)
        except json.JSONDecodeError as exc:
            return Indeterminate(
                reason="UNKNOWN",
                detail=f"JSON no parseable: {exc}",
            )

        metadata = _extract_json_metadata(data)
        group_sentido = _extract_json_group_sentido(data)

        if source_tag == "dip_post":
            post_evidence = "json_payload"
    else:
        soup = BeautifulSoup(text, "html.parser")

        metadata = _extract_metadata_html(soup)
        group_sentido = _extract_table_group_sentido(soup)
        form_meta = _extract_vote_form_meta(soup)
        meta.update(form_meta)

        if source_tag == "dip_post":
            post_evidence = _detect_post_evidence_html(
                soup,
                has_metadata=bool(metadata),
                has_vote_table=bool(group_sentido),
            )
            if post_evidence is None:
                meta["post_evidence"] = "none"

    if not group_sentido and not metadata:
        return Indeterminate(
            reason="UNKNOWN",
            detail="estructura no reconocida",
        )

    result: ParsedGaceta = {
        "source_tag": source_tag,
        "parser_version": parser_version,
        "metadata": metadata,
        "group_sentido": group_sentido,
    }

    if post_evidence is not None:
        result["post_evidence"] = post_evidence
    if meta:
        result["meta"] = meta

    return result


if __name__ == "__main__":
    html_example = b"""<!DOCTYPE html>
<html>
<head><title>Dictamen de Iniciativa</title></head>
<body>
<h1>Asunto: Iniciativa de Ley de Ejemplo</h1>
<p>Fecha de publicacion: 15/03/2024</p>
<table>
  <tr>
    <th>Grupo Parlamentario</th>
    <th>A Favor</th>
    <th>En Contra</th>
    <th>Abstencion</th>
    <th>Ausente</th>
  </tr>
  <tr>
    <td>Morena</td><td>150</td><td>0</td><td>5</td><td>10</td>
  </tr>
  <tr>
    <td>PAN</td><td>20</td><td>80</td><td>10</td><td>20</td>
  </tr>
</table>
</body>
</html>
"""
    print("HTML:", parse_response(html_example, "dip_gaceta", "dip_gaceta_0.1.0"))

    json_example = (
        b'{"asunto": "Iniciativa X", "fecha": "2024-03-15", '
        b'"votos": {"Morena": {"a_favor": 150, "en_contra": 0, '
        b'"abstencion": 5, "ausente": 10}, '
        b'"PAN": {"a_favor": 20, "en_contra": 80, '
        b'"abstencion": 10, "ausente": 20}}}'
    )
    print("JSON:", parse_response(json_example, "dip_post", "dip_post_0.1.0"))

    waf_example = (
        b"<html><title>Attention Required! | Cloudflare</title>"
        b"<body>Access Denied</body></html>"
    )
    print("WAF:", parse_response(waf_example, "dip_gaceta", "dip_gaceta_0.1.0"))

    unknown_example = b"<html><body><p>Hello world</p></body></html>"
    print("Unknown:", parse_response(unknown_example, "dip_post", "dip_post_0.1.0"))

    # Gaceta-style transposed table (rows=sentido, columns=grupo)
    transposed_html = b"""<html><body BGCOLOR="#FFFFFF">
<title>Gaceta Parlamentaria de la Camara de Diputados</title>
<form method="post" action="/voto66/ordi11/lanordi11.php3">
<input type="hidden" name="evento" value="16">
<table align=center border cellspacing=2 cellpadding=0 width=600>
<tr cellpadding=10>
<td width="14%" valign="top"><b>Votos</b></td>
<td width="14%" valign="top"><center><b>Total</b></center></td>
<td width="9%" valign="top"><center><b> MRN </b></center></td>
<td width="9%" valign="top"><center><b> PAN </b></center></td>
</tr>
<tr>
<td width="14%" valign="top"><FONT COLOR="#000099">Favor</font></td>
<td width="14%" valign="top"><center><input type="submit" name="lola[11]" value="350"></center></td>
<td width="9%" valign="top"><center><input type="submit" name="lola[21]" value="244"></center></td>
<td width="9%" valign="top"><center><input type="button" border="5" value=" 0 "></center></td>
</tr>
<tr>
<td><FONT COLOR="#000099">Contra</font></td>
<td><center><input type="submit" name="lola[12]" value="111"></center></td>
<td><center><input type="button" border="5" VALUE=" 0 "></center></td>
<td><center><input type="submit" name="lola[32]" value="58"></center></td>
</tr>
<tr>
<td><FONT COLOR="#000099">Abstenci&oacute;n</font></td>
<td><center><input type="submit" name="lola[13]" value="1"></center></td>
<td><center><input type="submit" name="lola[23]" value="1"></center></td>
<td><center><input type="button" border="5" value=" 0 "></center></td>
</tr>
<tr>
<td><FONT COLOR="#000099">Ausente</font></td>
<td><center><input type="submit" name="lola[15]" value="37"></center></td>
<td><center><input type="submit" name="lola[25]" value="7"></center></td>
<td><center><input type="submit" name="lola[35]" value="13"></center></td>
</tr>
<tr>
<td><FONT COLOR="#000099">Total</font></td>
<td><center>499</center></td>
<td><center>252</center></td>
<td><center>71</center></td>
</tr>
</table>
</form>
</body></html>"""
    print("Transposed:", parse_response(transposed_html, "dip_gaceta", "dip_gaceta_0.1.1"))

    # Intermediate/metadata-only page (like current raw POST response)
    intermediate_html = b"""<html>
<head><title>C\xe1mara de Diputados, LXVI Legislatura</title></head>
<body>
<table width="660"><tr>
<td width="90%" valign="top">
<font color="#990000"><font size="-1">
Dictamen de las Comisiones Unidas de ...
</font></font>
</td></tr></table>
<hr><center><font color="#990000"><font size="-1">
Gaceta Parlamentaria, LXVI Legislatura, C\xe1mara de Diputados, Ciudad de M\xe9xico
<hr></font></font></center>
</body></html>"""
    print(
        "Intermediate:",
        parse_response(intermediate_html, "dip_post", "dip_post_0.1.1"),
    )

    # ------------------------------------------------------------------
    # Test parse_tabla_agregada against the transposed HTML example
    # ------------------------------------------------------------------
    tabla_result = parse_tabla_agregada(
        transposed_html, "dip_gaceta", "dip_gaceta_0.1.1"
    )
    print("\n--- parse_tabla_agregada (transposed) ---")
    print(tabla_result)

    # Validation assertions
    assert "group_sentido" in tabla_result, "Missing group_sentido"
    gs = tabla_result["group_sentido"]  # type: ignore[index]
    assert gs["Total"]["a_favor"] == 350, f"Expected Total a_favor=350, got {gs['Total']['a_favor']}"
    assert gs["Total"]["en_contra"] == 111, f"Expected Total en_contra=111, got {gs['Total']['en_contra']}"
    assert gs["Total"]["abstencion"] == 1, f"Expected Total abstencion=1, got {gs['Total']['abstencion']}"
    assert gs["Total"]["ausente"] == 37, f"Expected Total ausente=37, got {gs['Total']['ausente']}"
    assert gs["MRN"]["a_favor"] == 244, f"Expected MRN a_favor=244, got {gs['MRN']['a_favor']}"
    assert gs["MRN"]["en_contra"] == 0, f"Expected MRN en_contra=0, got {gs['MRN']['en_contra']}"
    assert gs["MRN"]["abstencion"] == 1, f"Expected MRN abstencion=1, got {gs['MRN']['abstencion']}"
    assert gs["MRN"]["ausente"] == 7, f"Expected MRN ausente=7, got {gs['MRN']['ausente']}"
    assert gs["PAN"]["a_favor"] == 0, f"Expected PAN a_favor=0, got {gs['PAN']['a_favor']}"
    assert gs["PAN"]["en_contra"] == 58, f"Expected PAN en_contra=58, got {gs['PAN']['en_contra']}"
    assert gs["PAN"]["abstencion"] == 0, f"Expected PAN abstencion=0, got {gs['PAN']['abstencion']}"
    assert gs["PAN"]["ausente"] == 13, f"Expected PAN ausente=13, got {gs['PAN']['ausente']}"

    # lola_mapping validation
    assert "meta" in tabla_result, "Missing meta"
    meta = tabla_result["meta"]  # type: ignore[index]
    lola_map = json.loads(meta["lola_mapping"])
    assert lola_map["lola[11]"]["grupo"] == "Total"
    assert lola_map["lola[11]"]["sentido"] == "a_favor"
    assert lola_map["lola[11]"]["value"] == 350
    assert lola_map["lola[21]"]["grupo"] == "MRN"
    assert lola_map["lola[21]"]["sentido"] == "a_favor"
    assert lola_map["lola[21]"]["value"] == 244
    assert lola_map["lola[12]"]["grupo"] == "Total"
    assert lola_map["lola[12]"]["sentido"] == "en_contra"
    assert lola_map["lola[12]"]["value"] == 111
    assert lola_map["lola[32]"]["grupo"] == "PAN"
    assert lola_map["lola[32]"]["sentido"] == "en_contra"
    assert lola_map["lola[32]"]["value"] == 58
    assert lola_map["lola[13]"]["grupo"] == "Total"
    assert lola_map["lola[13]"]["sentido"] == "abstencion"
    assert lola_map["lola[13]"]["value"] == 1
    assert lola_map["lola[23]"]["grupo"] == "MRN"
    assert lola_map["lola[23]"]["sentido"] == "abstencion"
    assert lola_map["lola[23]"]["value"] == 1
    assert lola_map["lola[15]"]["grupo"] == "Total"
    assert lola_map["lola[15]"]["sentido"] == "ausente"
    assert lola_map["lola[15]"]["value"] == 37
    assert lola_map["lola[25]"]["grupo"] == "MRN"
    assert lola_map["lola[25]"]["sentido"] == "ausente"
    assert lola_map["lola[25]"]["value"] == 7
    assert lola_map["lola[35]"]["grupo"] == "PAN"
    assert lola_map["lola[35]"]["sentido"] == "ausente"
    assert lola_map["lola[35]"]["value"] == 13
    print("All assertions passed for parse_tabla_agregada.")
