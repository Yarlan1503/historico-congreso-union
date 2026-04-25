"""Registro de fuentes built-in del pipeline."""

from __future__ import annotations

from scraper.source_registry import SourceInfo, register_source
from f2.models import Chamber


def _register_builtins() -> None:
    """Registra todas las fuentes built-in."""
    # --- Diputados LXVI ---
    # Importaciones lazy para evitar circular imports
    from f1.parsers.xp_diputados_sitl import parse_response as sitl_parse
    from f1.parsers.xp_diputados_gaceta import (
        parse_tabla_agregada,
        parse_response as gaceta_parse,
    )
    from f1.parsers.xp_senado_lxvi import parse_response as senado_parse
    from f1.parsers.xp_senado_historico import parse_response as senado_hist_parse

    register_source(SourceInfo(
        tag="dip_sitl",
        chamber=Chamber.DIPUTADOS,
        legislature="LXVI",
        parser_func=sitl_parse,
        parser_suffix="dip_sitl",
        description="SITL/INFOPAL LXVI - Cámara de Diputados",
    ))
    register_source(SourceInfo(
        tag="dip_infopal",
        chamber=Chamber.DIPUTADOS,
        legislature="LXVI",
        parser_func=sitl_parse,
        parser_suffix="dip_sitl",
        description="INFOPAL LXVI (alias de dip_sitl)",
    ))
    register_source(SourceInfo(
        tag="dip_gaceta_tabla",
        chamber=Chamber.DIPUTADOS,
        legislature="LXVI",
        parser_func=parse_tabla_agregada,
        parser_suffix="dip_gaceta_tabla",
        description="Gaceta Parlamentaria LXVI - Tabla agregada",
    ))
    register_source(SourceInfo(
        tag="dip_gaceta_post",
        chamber=Chamber.DIPUTADOS,
        legislature="LXVI",
        parser_func=gaceta_parse,
        parser_suffix="dip_gaceta_post",
        description="Gaceta Parlamentaria LXVI - POST nominal",
    ))

    # --- Senado LXVI ---
    register_source(SourceInfo(
        tag="sen_lxvi_ajax",
        chamber=Chamber.SENADO,
        legislature="LXVI",
        parser_func=senado_parse,
        parser_suffix="sen_lxvi_ajax",
        description="Senado LXVI - Endpoint AJAX",
    ))
    register_source(SourceInfo(
        tag="senado_lxvi_ajax",
        chamber=Chamber.SENADO,
        legislature="LXVI",
        parser_func=senado_parse,
        parser_suffix="sen_lxvi_ajax",
        description="Senado LXVI - Endpoint AJAX (alias)",
    ))
    register_source(SourceInfo(
        tag="sen_lxvi_html",
        chamber=Chamber.SENADO,
        legislature="LXVI",
        parser_func=senado_parse,
        parser_suffix="sen_lxvi_html",
        description="Senado LXVI - Página HTML fallback",
    ))
    register_source(SourceInfo(
        tag="senado_lxvi_html",
        chamber=Chamber.SENADO,
        legislature="LXVI",
        parser_func=senado_parse,
        parser_suffix="sen_lxvi_html",
        description="Senado LXVI - Página HTML fallback (alias)",
    ))

    # --- Senado Histórico ---
    for leg in ("LXII", "LXIII", "LXIV", "LXV"):
        register_source(SourceInfo(
            tag=f"senado_historico_{leg.lower()}",
            chamber=Chamber.SENADO,
            legislature=leg,
            parser_func=senado_hist_parse,
            parser_suffix=f"senado_historico_{leg.lower()}",
            description=f"Senado histórico {leg} - HTML estático",
        ))

    # --- Senado Probes ---
    for leg in ("LX", "LXI"):
        register_source(SourceInfo(
            tag=f"senado_probe_{leg.lower()}",
            chamber=Chamber.SENADO,
            legislature=leg,
            parser_func=senado_hist_parse,
            parser_suffix=f"senado_probe_{leg.lower()}",
            description=f"Senado probe {leg} - Boundary test",
        ))


_register_builtins()
