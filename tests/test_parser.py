"""Tests para f1/parsers/xp_utils.py."""

import pytest

from f1.parsers.xp_utils import _detect_waf


def test_detect_waf_cloudflare():
    """Body con patrón de WAF debe devolver XPIndeterminate."""
    body = b"<html><title>Attention Required! | Cloudflare</title></html>"
    result = _detect_waf(body)
    assert result is not None
    assert result["reason"] == "WAF_CHALLENGE"
    assert "WAF" in result["detail"]


def test_detect_waf_clean_body():
    """Body limpio debe devolver None."""
    body = b"<html><body><p>Votaci\xc3\xb3n normal</p></body></html>"
    result = _detect_waf(body)
    assert result is None


def test_detect_waf_str_input():
    """Acepta entrada str además de bytes."""
    body = "Access Denied"
    result = _detect_waf(body)
    assert result is not None
    assert result["reason"] == "BLOCKED"
