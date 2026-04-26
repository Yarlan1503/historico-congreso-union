"""Normalización y disambiguación de nombres de legisladores.

Funciones puras de normalización para convertir nombres crudos del scraper
en claves canónicas deterministas. Sin dependencias externas.
"""
from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from difflib import SequenceMatcher, get_close_matches
from pathlib import Path
from typing import NamedTuple

# ---------------------------------------------------------------------------
# Regex patterns
# ---------------------------------------------------------------------------
_HONORIFIC_RE = re.compile(
    r"^(?:Sen\.|Dip\.|Senadora|Senador|Diputada|Diputado)\s*",
    re.IGNORECASE,
)
_SUFFIX_RE = re.compile(
    r"\s*\((?:LICENCIA|LIC|SUPLENTE|SUP|PROPIETARIO|PROP)\)\s*$",
    re.IGNORECASE,
)
_SPACE_BEFORE_COMMA_RE = re.compile(r"\s+,")


# ---------------------------------------------------------------------------
# 1. STRIP HONORIFICS
# ---------------------------------------------------------------------------
def strip_honorifics(name: str) -> str:
    """Quita prefijos honoríficos: Sen., Dip., Senadora, etc.

    Examples:
        >>> strip_honorifics("Sen. García, Juan")
        'García, Juan'
        >>> strip_honorifics("Dip. Chanona Burguete, Alejandro")
        'Chanona Burguete, Alejandro'
        >>> strip_honorifics("García, Juan")
        'García, Juan'
    """
    return _HONORIFIC_RE.sub("", name)


# ---------------------------------------------------------------------------
# 2. FIX MOJIBAKE
# ---------------------------------------------------------------------------
def fix_mojibake(name: str) -> str:
    """Detecta y corrige mojibake (UTF-8 interpretado como Latin-1).

    Detecta el patrón donde caracteres UTF-8 multibyte fueron interpretados
    como Latin-1 y luego re-encodificados. Ejemplo: 'Ã³' → 'ó'.

    Si el string ya está correcto, lo devuelve sin cambios.

    Examples:
        >>> fix_mojibake("GÃ³mez")
        'Gómez'
        >>> fix_mojibake("Gómez")
        'Gómez'
        >>> fix_mojibake("SÃ¡nchez")
        'Sánchez'
    """
    try:
        return name.encode("latin-1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return name


# ---------------------------------------------------------------------------
# 3. NORMALIZE WHITESPACE
# ---------------------------------------------------------------------------
def normalize_whitespace(name: str) -> str:
    """Normaliza espacios: colapsa múltiples espacios, arregla ' ,' → ','.

    Examples:
        >>> normalize_whitespace("García  López  Juan")
        'García López Juan'
        >>> normalize_whitespace("Ayala Almeida , Joel")
        'Ayala Almeida, Joel'
        >>> normalize_whitespace("  García  ")
        'García'
    """
    no_space_comma = _SPACE_BEFORE_COMMA_RE.sub(",", name)
    return " ".join(no_space_comma.split())


# ---------------------------------------------------------------------------
# 4. STRIP SUFFIXES
# ---------------------------------------------------------------------------
def strip_suffixes(name: str) -> str:
    """Quita sufijos parentéticos: (LICENCIA), (SUP), (PROP), etc.

    Examples:
        >>> strip_suffixes("Castro Trenti Fernando Jorge (LICENCIA)")
        'Castro Trenti Fernando Jorge'
        >>> strip_suffixes("García López Juan")
        'García López Juan'
    """
    return _SUFFIX_RE.sub("", name)


# ---------------------------------------------------------------------------
# 5. CANONICAL NAME
# ---------------------------------------------------------------------------
def canonical_name(name: str) -> str:
    """Pipeline completo de normalización: strip → fix → normalize → strip → lower.

    No quita acentos (se preservan para distinguir nombres).

    Examples:
        >>> canonical_name("Sen. Ayala Almeida , Joel")
        'ayala almeida, joel'
        >>> canonical_name("Castro Trenti Fernando Jorge (LICENCIA)")
        'castro trenti fernando jorge'
    """
    result = strip_honorifics(name)
    result = fix_mojibake(result)
    result = normalize_whitespace(result)
    result = strip_suffixes(result)
    # Remove residual commas after suffix stripping (e.g. "Hernández, Anais (LICENCIA)" → "Hernández Anais")
    result = result.replace(",", " ")
    return " ".join(result.split()).lower()


# ---------------------------------------------------------------------------
# 6. PERSON KEY (para uso del exporter)
# ---------------------------------------------------------------------------
_ACCENT_MAP = str.maketrans(
    "áéíóúÁÉÍÓÚñÑüÜ",
    "aeiouAEIOUnNuU",
)


def build_canonical_person_key(name: str) -> str:
    """Construye person_key determinista desde nombre con normalización completa.

    Pipeline: canonical_name → replace spaces with _ → strip accents.

    Examples:
        >>> build_canonical_person_key("Sen. García, Juan")
        'garcia,_juan'
        >>> build_canonical_person_key("Castro Trenti Fernando Jorge (LICENCIA)")
        'castro_trenti_fernando_jorge'
    """
    canon = canonical_name(name)
    no_spaces = canon.replace(" ", "_")
    return no_spaces.translate(_ACCENT_MAP)


# ---------------------------------------------------------------------------
# 7. DEEP NORMALIZE (para matching fuzzy)
# ---------------------------------------------------------------------------
def deep_normalize(name: str) -> str:
    """Canonical name + quitar acentos y comas para matching fuzzy.

    Útil para matching normalizado donde "garcía, juan" == "garcia juan".

    Examples:
        >>> deep_normalize("García, Juan")
        'garcia juan'
        >>> deep_normalize("Sen. López Martínez, Ana")
        'lopez martinez ana'
    """
    canon = canonical_name(name)
    no_commas = canon.replace(",", "")
    return no_commas.translate(_ACCENT_MAP)


# ---------------------------------------------------------------------------
# 8. CATALOG ENTRY
# ---------------------------------------------------------------------------
class CatalogEntry(NamedTuple):
    """Entrada del catálogo de personas."""

    canonical_name: str
    person_key: str
    original_names: str
    cast_count: int
    chambers: str
    party_senado: str
    n_variants: int
    is_ambiguous: bool


# ---------------------------------------------------------------------------
# 9. LOAD CATALOG
# ---------------------------------------------------------------------------
def load_catalog(csv_path: Path) -> dict[str, CatalogEntry]:
    """Carga catálogo CSV en dict person_key → CatalogEntry.

    Args:
        csv_path: Ruta al archivo CSV del catálogo.

    Returns:
        Diccionario con person_key como clave y CatalogEntry como valor.
    """
    catalog: dict[str, CatalogEntry] = {}
    with csv_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            entry = CatalogEntry(
                canonical_name=row["canonical_name"],
                person_key=row["person_key"],
                original_names=row["original_names"],
                cast_count=int(row["cast_count"]),
                chambers=row["chambers"],
                party_senado=row["party_senado"],
                n_variants=int(row["n_variants"]),
                is_ambiguous=row["is_ambiguous"].lower() == "true",
            )
            catalog[entry.person_key] = entry
    return catalog


# ---------------------------------------------------------------------------
# 10. MATCH PERSON
# ---------------------------------------------------------------------------
def match_person(name: str, catalog: dict[str, CatalogEntry]) -> PersonMatch:
    """Match un nombre contra el catálogo con fallback progresivo.

    Algoritmo en 3 niveles:
        1. Exact match: build_canonical_person_key(name) in catalog
        2. Normalized match: deep_normalize coincide con algún entry
        3. Fuzzy match: get_close_matches con cutoff=0.85
        4. No match → PersonMatch(method="new", ...)

    Args:
        name: Nombre crudo a buscar en el catálogo.
        catalog: Diccionario de person_key → CatalogEntry.

    Returns:
        PersonMatch con method, confidence, person_id si match, canonical_name.
    """
    canon = canonical_name(name)
    pkey = build_canonical_person_key(name)

    # 1. Exact match (probar con y sin comas — el catálogo no tiene comas en keys)
    for candidate in (pkey, pkey.replace(",", "")):
        if candidate in catalog:
            entry = catalog[candidate]
            return PersonMatch(
                method="exact",
                confidence=1.0,
                canonical_name=entry.canonical_name,
                person_id=entry.person_key,
                original_name=name,
            )

    # 2. Normalized match (deep normalize: sin acentos, sin comas)
    dn = deep_normalize(name)
    for _key, entry in catalog.items():
        if deep_normalize(entry.canonical_name) == dn:
            return PersonMatch(
                method="normalized",
                confidence=0.95,
                canonical_name=entry.canonical_name,
                person_id=entry.person_key,
                original_name=name,
            )

    # 3. Fuzzy match
    catalog_keys = list(catalog.keys())
    matches = get_close_matches(pkey, catalog_keys, n=1, cutoff=0.85)
    if matches:
        best_key = matches[0]
        entry = catalog[best_key]
        ratio = SequenceMatcher(None, pkey, best_key).ratio()
        return PersonMatch(
            method="fuzzy",
            confidence=round(ratio, 4),
            canonical_name=entry.canonical_name,
            person_id=entry.person_key,
            original_name=name,
        )

    # 4. No match
    return PersonMatch(
        method="new",
        confidence=0.0,
        canonical_name=canon,
        person_id=None,
        original_name=name,
    )


# ---------------------------------------------------------------------------
# 11. PersonMatch dataclass
# ---------------------------------------------------------------------------
@dataclass
class PersonMatch:
    """Resultado de matching de un nombre contra el catálogo."""

    method: str  # "exact", "normalized", "fuzzy", "new"
    confidence: float
    canonical_name: str
    person_id: str | None = None
    original_name: str = ""
