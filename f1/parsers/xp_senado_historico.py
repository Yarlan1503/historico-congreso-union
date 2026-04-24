"""Parser transductor temporal para fuente Senado histórico (LXII-LXV).

Incluye boundary probes LX/LXI.  Estrategia defensiva: nunca falla en silencio;
cualquier anomalía se convierte en ``Indeterminate`` o en una clasificación
explícita distinta de ``html_static``.
"""

from __future__ import annotations

import re

from bs4 import BeautifulSoup

from f1.parsers.xp_types import XPCounts, XPIndeterminate, XPParsedSenadoHistorico
from f1.parsers.xp_utils import _decode_body, _detect_waf

# ---------------------------------------------------------------------------
# Constantes de dominio
# ---------------------------------------------------------------------------

VALID_SOURCE_TAGS: set[str] = {
    "senado_historico_lxii",
    "senado_historico_lxiii",
    "senado_historico_lxiv",
    "senado_historico_lxv",
    "senado_probe_lx",
    "senado_probe_lxi",
}

_DYNAMIC_EMPTY_HINTS: tuple[str, ...] = (
    "cargando",
    "loading",
    "espere",
    "procesando",
    "no hay registros",
    "sin resultados",
)

_DOWNLOAD_EXTENSIONS: tuple[str, ...] = (
    ".pdf",
    ".csv",
    ".xls",
    ".xlsx",
)

# Patrones para extraer conteos del texto libre o tablas del Senado
_COUNT_PATTERNS = {
    "a_favor": re.compile(r"[Aa]\s*[Ff]avor\s*[:\-]?\s*(\d+)", re.IGNORECASE),
    "en_contra": re.compile(r"[Ee]n\s*[Cc]ontra\s*[:\-]?\s*(\d+)", re.IGNORECASE),
    "abstencion": re.compile(r"[Aa]bstencion(?:es)?\s*[:\-]?\s*(\d+)", re.IGNORECASE),
    "ausente": re.compile(r"[Aa]usente?s?\s*[:\-]?\s*(\d+)", re.IGNORECASE),
}

# ---------------------------------------------------------------------------
# Tipos alias (convención del proyecto)
# ---------------------------------------------------------------------------

Indeterminate = XPIndeterminate
ParsedSenadoHistorico = XPParsedSenadoHistorico

# ---------------------------------------------------------------------------
# Helpers privados
# ---------------------------------------------------------------------------


def _detect_dynamic_empty(soup: BeautifulSoup) -> bool:
    """Detecta si el HTML tiene estructura de tabla pero sin filas de datos."""
    tables = soup.find_all("table")
    if not tables:
        return False

    for tbl in tables:
        rows = tbl.find_all("tr")
        # Solo headers o filas vacías
        data_rows = [
            r
            for r in rows
            if r.find(["td"], recursive=False)  # type: ignore[arg-type]
        ]
        if not data_rows:
            return True

    # También señales textuales dentro de contenedores principales
    text = soup.get_text(separator=" ", strip=True).lower()
    return any(hint in text for hint in _DYNAMIC_EMPTY_HINTS)


def _detect_download_link(soup: BeautifulSoup) -> bool:
    """Detecta si la página contiene enlaces de descarga en lugar de tabla HTML."""
    links = soup.find_all("a", href=True)
    for a in links:
        href: str = a["href"]
        if any(href.lower().endswith(ext) for ext in _DOWNLOAD_EXTENSIONS):
            return True
    return False


def _extract_metadata(soup: BeautifulSoup) -> dict[str, str]:
    """Extrae metadatos básicos: título, descripción, fecha si aparece."""
    meta: dict[str, str] = {}

    title_tag = soup.find("title")
    if title_tag and title_tag.string:
        meta["title"] = title_tag.string.strip()

    # Meta description
    desc_tag = soup.find("meta", attrs={"name": "description"})
    if desc_tag and desc_tag.get("content"):
        meta["description"] = desc_tag["content"].strip()

    # Intentar capturar fecha de votación de texto libre
    text = soup.get_text(separator=" ", strip=True)
    fecha_match = re.search(
        r"(\d{1,2})\s+de\s+([a-zA-Z]+)\s+de\s+(\d{4})",
        text,
    )
    if fecha_match:
        meta["fecha_capturada"] = fecha_match.group(0)

    # Tipo de página
    if "/informacion/votaciones" in text:
        meta["page_type"] = "indice"
    elif "/votacion/" in text:
        meta["page_type"] = "detalle"
    else:
        meta["page_type"] = "desconocido"

    return meta


def _extract_counts(soup: BeautifulSoup) -> XPCounts | None:
    """Extrae conteos agregados del HTML estático.

    Busca primero en tablas con clases/ids comunes del Senado y luego
    recurre a expresiones regulares sobre el texto plano.
    """
    counts: dict[str, int] = {}

    # 1. Búsqueda estructurada en tablas
    tables = soup.find_all("table")
    for tbl in tables:
        for row in tbl.find_all("tr"):
            cells = row.find_all(["td", "th"])
            if len(cells) < 2:
                continue
            label = cells[0].get_text(strip=True).lower()
            value_text = cells[1].get_text(strip=True)
            try:
                value = int(value_text)
            except ValueError:
                continue
            if "favor" in label:
                counts["a_favor"] = value
            elif "contra" in label:
                counts["en_contra"] = value
            elif "abstencion" in label:
                counts["abstencion"] = value
            elif "ausente" in label:
                counts["ausente"] = value

    # 2. Fallback por regex sobre todo el texto
    if len(counts) < 4:
        text = soup.get_text(separator=" ", strip=True)
        for key, pattern in _COUNT_PATTERNS.items():
            if key not in counts:
                m = pattern.search(text)
                if m:
                    counts[key] = int(m.group(1))

    if len(counts) == 4:
        return XPCounts(
            a_favor=counts["a_favor"],
            en_contra=counts["en_contra"],
            abstencion=counts["abstencion"],
            ausente=counts["ausente"],
        )
    return None


# ---------------------------------------------------------------------------
# API pública
# ---------------------------------------------------------------------------


def parse_response(
    body: bytes,
    source_tag: str,
    parser_version: str,
) -> ParsedSenadoHistorico | Indeterminate:
    """Parsea una respuesta del histórico del Senado.

    Args:
        body: Carga bruta (bytes) de la respuesta HTTP.
        source_tag: Identificador de la fuente; debe pertenecer a
            ``VALID_SOURCE_TAGS``.
        parser_version: Versión semántica del parser, p. ej.
            ``"senado_historico_0.1.0"``.

    Returns:
        * ``XPParsedSenadoHistorico`` cuando la estructura se reconoce.
        * ``XPIndeterminate`` cuando hay bloqueo WAF/cache o la estructura
          es completamente irreconocible.
    """
    if source_tag not in VALID_SOURCE_TAGS:
        return Indeterminate(
            reason="BAD_SOURCE_TAG",
            detail=f"source_tag '{source_tag}' no está en {VALID_SOURCE_TAGS}",
        )

    text, _ = _decode_body(body)

    # --- Heurística de bloqueo (primer filtro, antes de parsear HTML) ---
    waf = _detect_waf(text)
    if waf is not None:
        return Indeterminate(
            source_tag=source_tag,
            parser_version=parser_version,
            reason=waf["reason"],
            detail=waf["detail"],
        )

    soup = BeautifulSoup(text, "html.parser")

    # Si el documento está vacío o es puramente un redirect script
    if not soup.find(["body", "html"]):
        return Indeterminate(
            reason="UNKNOWN",
            detail="estructura no reconocida",
        )

    metadata = _extract_metadata(soup)

    # --- Clasificación secundaria ---
    if _detect_dynamic_empty(soup):
        return ParsedSenadoHistorico(
            source_tag=source_tag,
            parser_version=parser_version,
            classification="html_dynamic_empty",
            counts=None,
            metadata=metadata,
        )

    if _detect_download_link(soup):
        return ParsedSenadoHistorico(
            source_tag=source_tag,
            parser_version=parser_version,
            classification="download_link",
            counts=None,
            metadata=metadata,
        )

    # --- Intento de extracción de conteos (html_static) ---
    counts = _extract_counts(soup)
    if counts is not None:
        return ParsedSenadoHistorico(
            source_tag=source_tag,
            parser_version=parser_version,
            classification="html_static",
            counts=counts,
            metadata=metadata,
        )

    # Si hay contenido HTML reconocible pero sin conteos, seguimos clasificando
    # como html_static si hay estructura de votación; de lo contrario unknown.
    if soup.find("table") or soup.find(class_=re.compile(r"votacion|voto", re.I)):
        return ParsedSenadoHistorico(
            source_tag=source_tag,
            parser_version=parser_version,
            classification="html_static",
            counts=None,
            metadata=metadata,
        )

    return Indeterminate(
        reason="UNKNOWN",
        detail="estructura no reconocida",
    )


# ---------------------------------------------------------------------------
# Ejemplos mínimos
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # 1. HTML estático con conteos
    html_static = b"""\
<html><head><title>Votacion 123</title></head>
<body>
<table>
  <tr><td>A favor</td><td>45</td></tr>
  <tr><td>En contra</td><td>30</td></tr>
  <tr><td>Abstencion</td><td>5</td></tr>
  <tr><td>Ausente</td><td>20</td></tr>
</table>
</body></html>
"""
    result = parse_response(
        html_static,
        source_tag="senado_historico_lxii",
        parser_version="senado_historico_0.1.0",
    )
    print("STATIC:", result)

    # 2. HTML vacío (dinámico)
    html_empty = b"""\
<html><head><title>Votaciones</title></head>
<body>
<table><tr><th>Fecha</th><th>Tema</th></tr></table>
<p>Cargando...</p>
</body></html>
"""
    result = parse_response(
        html_empty,
        source_tag="senado_historico_lxiii",
        parser_version="senado_historico_0.1.0",
    )
    print("EMPTY:", result)

    # 3. HTML de bloqueo (Incapsula)
    html_blocked = b"""\
<html><body>
<h1>Access Denied</h1>
<p>Your request has been blocked by Incapsula.</p>
</body></html>
"""
    result = parse_response(
        html_blocked,
        source_tag="senado_probe_lx",
        parser_version="senado_historico_0.1.0",
    )
    print("BLOCKED:", result)
