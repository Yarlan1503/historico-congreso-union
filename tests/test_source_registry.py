"""Tests para scraper/source_registry.py."""

import pytest

from f2.models import Chamber


def test_builtin_sources_registered():
    """Las fuentes built-in deben registrarse al importar el módulo."""
    import scraper._builtin_sources  # noqa: F401 — side effect import
    from scraper.source_registry import get_source

    info = get_source("dip_sitl")
    assert info.chamber == Chamber.DIPUTADOS
    assert info.legislature == "LXVI"
    assert info.parser_func is not None
    assert info.parser_suffix == "dip_sitl"


def test_get_parser_returns_callable():
    """get_parser debe devolver un callable y un suffix."""
    import scraper._builtin_sources  # noqa: F401
    from scraper.source_registry import get_parser

    func, suffix = get_parser("dip_sitl")
    assert callable(func)
    assert suffix == "dip_sitl"


def test_get_chamber_from_registry():
    """get_chamber debe inferir cámara desde el registry."""
    import scraper._builtin_sources  # noqa: F401
    from scraper.source_registry import get_chamber

    assert get_chamber("dip_sitl") == Chamber.DIPUTADOS
    assert get_chamber("sen_lxvi_ajax") == Chamber.SENADO
    assert get_chamber("dip_gaceta_tabla") == Chamber.DIPUTADOS


def test_get_chamber_fallback():
    """Tags no registrados deben inferir cámara por prefix."""
    from scraper.source_registry import get_chamber

    assert get_chamber("dip_sitl_lxv") == Chamber.DIPUTADOS
    assert get_chamber("sen_lxv_ajax") == Chamber.SENADO


def test_register_new_source():
    """Se puede registrar una fuente nueva sin modificar models.py."""
    import scraper._builtin_sources  # noqa: F401
    from scraper.source_registry import SourceInfo, get_source, register_source

    register_source(SourceInfo(
        tag="dip_test_custom",
        chamber=Chamber.DIPUTADOS,
        legislature="LXII",
        description="Test source",
    ))
    info = get_source("dip_test_custom")
    assert info.legislature == "LXII"
    assert info.chamber == Chamber.DIPUTADOS


def test_get_unknown_source_raises():
    """Un tag no registrado sin fallback debe lanzar ValueError."""
    from scraper.source_registry import get_source

    with pytest.raises(ValueError, match="no registrado"):
        get_source("fuente_totalmente_inventada_xyz")


def test_sources_by_legislature():
    """sources_by_legislature filtra correctamente."""
    import scraper._builtin_sources  # noqa: F401
    from scraper.source_registry import sources_by_legislature

    lxvi_sources = sources_by_legislature("LXVI")
    lxvi_tags = {s.tag for s in lxvi_sources}
    assert "dip_sitl" in lxvi_tags
    assert "sen_lxvi_ajax" in lxvi_tags


def test_sources_by_chamber():
    """sources_by_chamber filtra correctamente."""
    import scraper._builtin_sources  # noqa: F401
    from scraper.source_registry import sources_by_chamber

    dip_sources = sources_by_chamber(Chamber.DIPUTADOS)
    dip_tags = {s.tag for s in dip_sources}
    assert "dip_sitl" in dip_tags
    assert "dip_gaceta_post" in dip_tags
    # Senado sources no deben aparecer
    assert "sen_lxvi_ajax" not in dip_tags


def test_senado_historico_registered():
    """Las fuentes históricas del Senado deben estar registradas."""
    import scraper._builtin_sources  # noqa: F401
    from scraper.source_registry import get_source

    for leg in ("lxii", "lxiii", "lxiv", "lxv"):
        tag = f"senado_historico_{leg}"
        info = get_source(tag)
        assert info.chamber == Chamber.SENADO
        assert info.parser_func is not None


def test_senado_probes_registered():
    """Los probes de Senado LX/LXI deben estar registrados."""
    import scraper._builtin_sources  # noqa: F401
    from scraper.source_registry import get_source

    for leg in ("lx", "lxi"):
        tag = f"senado_probe_{leg}"
        info = get_source(tag)
        assert info.chamber == Chamber.SENADO
        assert info.parser_func is not None


def test_all_sources_returns_copy():
    """all_sources debe devolver una copia, no el registry interno."""
    import scraper._builtin_sources  # noqa: F401
    from scraper.source_registry import all_sources

    sources = all_sources()
    assert isinstance(sources, dict)
    assert len(sources) > 0
    # Modificar la copia no debe afectar el registry
    sources["fake"] = None  # type: ignore[assignment]
    from scraper.source_registry import get_source
    with pytest.raises(ValueError):
        get_source("fake")


def test_get_parser_no_parser_raises():
    """Un source registrado sin parser_func debe lanzar ValueError."""
    from scraper.source_registry import SourceInfo, get_parser, register_source

    register_source(SourceInfo(
        tag="test_no_parser",
        chamber=Chamber.SENADO,
        legislature="LX",
        description="Sin parser",
    ))
    with pytest.raises(ValueError, match="no tiene parser asociado"):
        get_parser("test_no_parser")
