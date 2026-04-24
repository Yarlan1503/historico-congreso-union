"""Tests para scraper/pipeline.py."""

from datetime import datetime, timezone

import pytest

from scraper._types import FetchResult
from scraper.pipeline import get_parser_module, process


class TestGetParserModule:
    """Suite de tests para get_parser_module."""

    def test_get_parser_module_routing(self):
        """Debe devolver el parser correcto para fuentes conocidas y rechazar desconocidas."""
        func_dip, suffix_dip = get_parser_module("dip_sitl")
        assert callable(func_dip)
        assert suffix_dip == "dip_sitl"

        func_sen, suffix_sen = get_parser_module("sen_lxvi_ajax")
        assert callable(func_sen)
        assert suffix_sen == "sen_lxvi_ajax"

    def test_get_parser_module_unknown_raises(self):
        """Un source_tag desconocido debe lanzar ValueError."""
        with pytest.raises(ValueError, match="source_tag no reconocido"):
            get_parser_module("fuente_inventada_123")


class TestProcessClassifications:
    """Suite de tests para process."""

    @pytest.fixture
    def base_fetch_result(self):
        """FetchResult mínimo para usar como base en los tests."""
        return FetchResult(
            url="https://example.com/test",
            method="GET",
            status_code=200,
            body=b"",
            headers={},
            latency_ms=0.0,
            timestamp=datetime.now(timezone.utc),
            sha256_body="a" * 64,
            sha256_headers=None,
        )

    def test_process_success(self, base_fetch_result):
        """Un body con HTML parseable por SITL debe clasificarse como SUCCESS."""
        html = (
            b"<html><body>"
            b'<div class="resultados">'
            b"<p>A FAVOR: 20</p>"
            b"<p>EN CONTRA: 10</p>"
            b"<p>ABSTENCION: 3</p>"
            b"<p>AUSENTE: 2</p>"
            b"<p>NO VOTO: 0</p>"
            b"</div>"
            b"</body></html>"
        )
        fetch_result = FetchResult(
            url=base_fetch_result.url,
            method=base_fetch_result.method,
            status_code=base_fetch_result.status_code,
            body=html,
            headers=base_fetch_result.headers,
            latency_ms=base_fetch_result.latency_ms,
            timestamp=base_fetch_result.timestamp,
            sha256_body=base_fetch_result.sha256_body,
            sha256_headers=base_fetch_result.sha256_headers,
        )

        result = process(fetch_result, "dip_sitl")
        assert result.classification == "SUCCESS"
        assert result.parsed_data is not None
        assert "counts" in result.parsed_data
        assert len(result.counts) == 1
        assert result.counts[0]["total"] == 35

    def test_process_indeterminate(self, base_fetch_result):
        """Un body vacío o sin estructura reconocible debe clasificarse como INDETERMINATE."""
        fetch_result = FetchResult(
            url=base_fetch_result.url,
            method=base_fetch_result.method,
            status_code=base_fetch_result.status_code,
            body=b"<html><body><p>Sin datos</p></body></html>",
            headers=base_fetch_result.headers,
            latency_ms=base_fetch_result.latency_ms,
            timestamp=base_fetch_result.timestamp,
            sha256_body=base_fetch_result.sha256_body,
            sha256_headers=base_fetch_result.sha256_headers,
        )

        result = process(fetch_result, "dip_sitl")
        assert result.classification == "INDETERMINATE"
        assert result.parser_errors
