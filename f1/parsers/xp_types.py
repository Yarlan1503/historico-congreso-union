"""Tipos temporales base para parsers transductores (prefijo XP)."""

from __future__ import annotations

from typing import Literal, NotRequired, TypedDict


XPSentido = Literal[
    "a_favor", "en_contra", "abstencion", "ausente", "novoto", "presente"
]


class XPVoteCast(TypedDict):
    """Registro nominal de voto individual."""

    iddip: str
    partidot: str
    sentido: XPSentido


class XPCounts(TypedDict):
    """Conteos agregados por categoría de voto."""

    a_favor: int
    en_contra: int
    abstencion: int
    ausente: int
    quorum: NotRequired[int]
    novoto: NotRequired[int]


class XPSenatorCast(TypedDict):
    """Registro nominal de voto individual para Senado."""

    nombre: str
    grupo: str
    sentido: XPSentido


class XPParsedSenadoLXVI(TypedDict):
    """Resultado exitoso del parser para fuente Senado LXVI."""

    source_tag: str
    parser_version: str
    metadata: dict[str, str]
    counts: XPCounts
    nominal: list[XPSenatorCast]
    meta: NotRequired[dict[str, str]]


class XPIndeterminate(TypedDict):
    """Resultado indeterminado cuando el parser no puede extraer datos útiles."""

    source_tag: NotRequired[str]
    parser_version: NotRequired[str]
    reason: str
    detail: str


class XPParsedGaceta(TypedDict):
    """Resultado parseado de una gaceta o POST de la Cámara de Diputados."""

    source_tag: str
    parser_version: str
    metadata: dict[str, str]
    group_sentido: dict[str, XPCounts]
    post_evidence: NotRequired[str]
    meta: NotRequired[dict[str, str]]


class XPParsedSenadoHistorico(TypedDict):
    """Resultado parseado del histórico del Senado (LXII-LXV) o boundary probes."""

    source_tag: str
    parser_version: str
    classification: Literal[
        "html_static",
        "html_dynamic_empty",
        "download_link",
        "blocked",
    ]
    counts: XPCounts | None
    metadata: dict[str, str]
    meta: NotRequired[dict[str, str]]


class XPParsedCounts(TypedDict):
    """Resultado exitoso del parser con conteos y lista nominal opcional."""

    source_tag: str
    parser_version: str
    counts: XPCounts
    nominal: list[XPVoteCast]
    meta: NotRequired[dict[str, str]]


ParsedSenadoLXVI = XPParsedSenadoLXVI
ParsedSenadoHistorico = XPParsedSenadoHistorico
SenatorCast = XPSenatorCast
Indeterminate = XPIndeterminate
ParsedCounts = XPParsedCounts
