"""Tests para shared/transform_bridge.py."""

import pytest

from shared.transform_bridge import build_counts, normalize_sentido


class TestBuildCounts:
    """Suite de tests para build_counts."""

    def test_build_counts_calculates_total(self):
        """Los conteos simples deben sumar correctamente el total."""
        parsed = {"counts": {"a_favor": 10, "en_contra": 5, "abstencion": 2}}
        result = build_counts(parsed)

        assert len(result) == 1
        entry = result[0]
        assert entry["group_name"] is None
        assert entry["a_favor"] == 10
        assert entry["en_contra"] == 5
        assert entry["abstencion"] == 2
        assert entry["ausente"] == 0
        assert entry["novoto"] == 0
        assert entry["presente"] == 0
        assert entry["total"] == 17

    def test_build_counts_group_sentido(self):
        """Los conteos agrupados deben calcular totales por grupo."""
        parsed = {
            "group_sentido": {
                "Grupo A": {"a_favor": 3, "en_contra": 1},
                "Grupo B": {"abstencion": 2, "ausente": 1},
            }
        }
        result = build_counts(parsed)

        assert len(result) == 2
        by_name = {r["group_name"]: r for r in result}

        assert by_name["Grupo A"]["a_favor"] == 3
        assert by_name["Grupo A"]["en_contra"] == 1
        assert by_name["Grupo A"]["total"] == 4

        assert by_name["Grupo B"]["abstencion"] == 2
        assert by_name["Grupo B"]["ausente"] == 1
        assert by_name["Grupo B"]["total"] == 3


class TestNormalizeSentido:
    """Suite de tests para normalize_sentido."""

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("A FAVOR", "a_favor"),
            ("EN CONTRA", "en_contra"),
            ("ABSTENCION", "abstencion"),
            ("a favor", "a_favor"),
            ("en contra", "en_contra"),
            ("abstención", "abstencion"),
        ],
    )
    def test_normalize_sentido_maps_correctly(self, raw, expected):
        """Los sentidos canónicos deben mapearse correctamente."""
        assert normalize_sentido(raw, source_tag="dip_sitl") == expected

    def test_normalize_sentido_presente_sitl(self):
        """'presente' con source_tag dip_sitl debe mapearse a a_favor."""
        assert normalize_sentido("presente", source_tag="dip_sitl") == "a_favor"
        assert normalize_sentido("PRESENTE", source_tag="dip_infopal") == "a_favor"

    def test_normalize_sentido_unknown_returns_none(self):
        """Un valor desconocido debe devolver None."""
        assert normalize_sentido("voto_secreto", source_tag="dip_sitl") is None
        assert normalize_sentido("", source_tag="sen_lxvi_ajax") is None
