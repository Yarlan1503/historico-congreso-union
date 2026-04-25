"""Registry centralizado de fuentes de datos del Congreso."""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass

from f2.models import Chamber

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SourceInfo:
    """Metadata de una fuente de datos registrada."""

    tag: str                              # e.g. "dip_sitl", "sen_lxvi_ajax"
    chamber: Chamber                      # Cámara legislativa
    legislature: str                      # e.g. "LXVI", "LXV"
    parser_func: Callable | None = None   # Función parser para este source_tag
    parser_suffix: str = ""               # Suffix para parser_version
    scraper_func: Callable | None = None  # Función scraper (opcional)
    description: str = ""                 # Descripción humana


# Registry global
_REGISTRY: dict[str, SourceInfo] = {}


def register_source(info: SourceInfo) -> None:
    """Registra una fuente en el registry global."""
    if info.tag in _REGISTRY:
        logger.warning("Source tag '%s' ya registrado, sobrescribiendo", info.tag)
    _REGISTRY[info.tag] = info


def get_source(tag: str) -> SourceInfo:
    """Recupera info de una fuente registrada.

    Raises:
        ValueError: si el tag no está registrado.
    """
    info = _REGISTRY.get(tag)
    if info is None:
        raise ValueError(f"source_tag no registrado: {tag!r}")
    return info


def get_parser(tag: str) -> tuple[Callable, str]:
    """Devuelve (parser_func, suffix) para un source_tag.

    Raises:
        ValueError: si el tag no tiene parser asociado.
    """
    info = get_source(tag)
    if info.parser_func is None:
        raise ValueError(f"source_tag '{tag}' no tiene parser asociado")
    return info.parser_func, info.parser_suffix


def get_chamber(tag: str) -> Chamber:
    """Infiere la cámara desde el registry.

    Fallback a prefix matching si el tag no está registrado.
    """
    info = _REGISTRY.get(tag)
    if info is not None:
        return info.chamber
    # Fallback genérico
    tag_lower = tag.lower().strip()
    if tag_lower.startswith("dip_"):
        return Chamber.DIPUTADOS
    if tag_lower.startswith("sen_") or tag_lower.startswith("senado_"):
        return Chamber.SENADO
    return Chamber.SENADO  # default


def all_sources() -> dict[str, SourceInfo]:
    """Devuelve una copia del registry completo."""
    return dict(_REGISTRY)


def sources_by_legislature(legislature: str) -> list[SourceInfo]:
    """Filtra fuentes por legislatura."""
    return [info for info in _REGISTRY.values() if info.legislature == legislature]


def sources_by_chamber(chamber: Chamber) -> list[SourceInfo]:
    """Filtra fuentes por cámara."""
    return [info for info in _REGISTRY.values() if info.chamber == chamber]
