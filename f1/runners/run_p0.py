#!/usr/bin/env python3
"""P0 Runner — Validación empírica controlada de packets Fase 1.

Ejecución:
    python f1/runners/run_p0.py [--packet-id ID [ID ...]] [--output-dir DIR]

Requiere Python 3.11+ (usa tomllib).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import random
import re
import sys
import time
import tomllib
import urllib.parse
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

from f1.parsers import xp_diputados_gaceta, xp_diputados_sitl, xp_senado_lxvi

# Parser opcional para históricos (no todos los entornos lo tienen)
try:
    from f1.parsers import xp_senado_historico
except Exception:  # pragma: no cover
    xp_senado_historico = None  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
CONFIG_PATH = PROJECT_ROOT / "f1" / "config" / "xp_config.toml"
PACKETS_DIR = PROJECT_ROOT / "f1" / "packets"
PARSER_VERSION = "p0_runner_0.1.0"

# Mapeo de source_tag → módulo parser
PARSER_MAP: dict[str, Any] = {
    "dip_sitl": xp_diputados_sitl,
    "dip_infopal": xp_diputados_sitl,
    "dip_gaceta": xp_diputados_gaceta,
    "dip_post": xp_diputados_gaceta,
    "senado_lxvi_html": xp_senado_lxvi,
    "senado_lxvi_ajax": xp_senado_lxvi,
}

if xp_senado_historico is not None:
    for _tag in getattr(xp_senado_historico, "VALID_SOURCE_TAGS", set()):
        PARSER_MAP[_tag] = xp_senado_historico


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def load_config(path: Path) -> dict[str, Any]:
    """Carga configuración TOML desde path."""
    with path.open("rb") as fh:
        return tomllib.load(fh)


def discover_packets(
    packet_dir: Path, filter_ids: list[str] | None = None
) -> list[dict[str, Any]]:
    """Descubre packets JSON en el directorio dado."""
    packets: list[dict[str, Any]] = []
    for p in sorted(packet_dir.glob("XP_*.json")):
        with p.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
        if filter_ids is None or data.get("packet_id") in filter_ids:
            packets.append(data)
    return packets


def build_url(packet: dict[str, Any]) -> str:
    """Construye la URL final de un packet."""
    url = packet.get("url")
    if url:
        return url
    url_pattern = packet.get("url_pattern")
    parameters = packet.get("parameters") or {}
    if url_pattern:
        return url_pattern.format(**parameters)
    raise ValueError(f"Packet {packet['packet_id']} sin url ni url_pattern")


def get_timeout_key(url: str, source_tag: str, packet_id: str = "") -> str:
    """Determina la categoría de timeout para una URL."""
    lower = url.lower()
    if "viewtablevot" in lower or "ajax" in lower:
        return "ajax"
    if re.search(r"\.(pdf|zip|xlsx?|csv|docx?)$", lower):
        return "download"
    if "index" in source_tag.lower() or "listado" in source_tag.lower():
        return "index"
    if "_INDEX" in packet_id:
        return "index"
    parsed = urllib.parse.urlparse(url)
    path = parsed.path
    query = parsed.query.lower()
    if path.endswith("/"):
        return "index"
    last_segment = path.split("/")[-1]
    if last_segment and (
        re.search(r"^\d+$", last_segment)
        or re.search(r"\d{4}[-/]\d{2}[-/]\d{2}", last_segment)
        or re.search(r"\.(html?|php|asp|jsp)$", last_segment, re.I)
    ):
        return "detail"
    if not any(k in query for k in ("id=", "numero=", "vote_id=", "fecha=")):
        return "index"
    return "detail"


def get_repetitions(packet: dict[str, Any], config: dict[str, Any]) -> int:
    """Obtiene número de repeticiones desde config o packet."""
    risks = packet.get("risks_covered", [])
    has_waf = any("waf" in str(r).lower() for r in risks)
    default_key = "riesgo_WAF" if has_waf else "estable"
    default_reps = config.get("max_repetitions", {}).get(default_key, 2)
    return packet.get("repetitions", default_reps)


def decode_body_safe(body: bytes) -> str:
    """Decodifica bytes a str probando utf-8 e iso-8859-1."""
    for enc in ("utf-8", "iso-8859-1"):
        try:
            return body.decode(enc)
        except UnicodeDecodeError:
            continue
    return body.decode("utf-8", errors="replace")


def check_waf_from_response(
    response: httpx.Response,
    packet: dict[str, Any],
    config: dict[str, Any],
) -> tuple[bool, str | None]:
    """Devuelve (waf_detected, reason_string)."""
    source_tag = packet["source_tag"]
    triggers = config.get("indeterminate_triggers", {})
    tag_triggers = triggers.get(source_tag, {})

    body_text = decode_body_safe(response.content)
    lowered_body = body_text.lower()

    # 1. Status codes bloqueantes
    if response.status_code in (403, 502, 503, 429):
        return True, f"STATUS_{response.status_code}"

    # 2. Body substrings
    for sub in tag_triggers.get("body_substrings", []):
        if sub.lower() in lowered_body:
            return True, f"BODY_SUBSTRING_{sub}"

    # 3. Header values / names
    header_vals = tag_triggers.get("header_values", [])
    for hval in header_vals:
        hlower = hval.lower()
        for hname, hvalue in response.headers.items():
            if hlower in hname.lower() or hlower in hvalue.lower():
                return True, f"HEADER_{hval}"

    # 4. WAF selectivo por path (senado)
    url = build_url(packet)
    parsed = urllib.parse.urlparse(url)
    host = parsed.hostname or ""
    if "senado.gob.mx" in host:
        selective = triggers.get("senado_path_selective", {})
        blocked_paths = selective.get("blocked_path_patterns", [])
        path = parsed.path or ""
        for bp in blocked_paths:
            if path.startswith(bp) and (len(path) == len(bp) or path[len(bp)] == "/"):
                # Solo si también hay indicios de bloqueo
                if response.status_code in (403, 502, 503, 429, 0):
                    return True, "SENADO_PATH_SELECTIVE_WAF"
                for sub in selective.get("body_substrings", []):
                    if sub.lower() in lowered_body:
                        return True, "SENADO_PATH_SELECTIVE_WAF"
                for hval in selective.get("header_values", []):
                    hlower = hval.lower()
                    for hname, hvalue in response.headers.items():
                        if hlower in hname.lower() or hlower in hvalue.lower():
                            return True, "SENADO_PATH_SELECTIVE_WAF"
                break

    return False, None


def check_cache_detected(
    response: httpx.Response, config: dict[str, Any], source_tag: str
) -> bool:
    """Detecta si la respuesta proviene de cache."""
    triggers = config.get("indeterminate_triggers", {})
    tag_triggers = triggers.get(source_tag, {})
    header_vals = tag_triggers.get("header_values", [])
    for hval in header_vals:
        if "cache" in hval.lower():
            hlower = hval.lower()
            for hname, hvalue in response.headers.items():
                if hlower in hname.lower() or hlower in hvalue.lower():
                    return True
    # Comprobaciones generales
    for hname, hvalue in response.headers.items():
        hl = hname.lower()
        cache_headers = ("x-cache", "cf-cache-status", "x-drupal-cache", "x-varnish")
        if hl in cache_headers and "hit" in hvalue.lower():
            return True
    return False


def save_attempt(
    output_dir: Path,
    packet_id: str,
    attempt_num: int,
    response: httpx.Response,
    request_headers: dict[str, str],
    request_payload: bytes,
    meta: dict[str, Any],
) -> None:
    """Persiste en disco los datos de un intento."""
    base = output_dir / "xraw" / packet_id / f"attempt_{attempt_num}"
    base.mkdir(parents=True, exist_ok=True)

    (base / "response_body.bin").write_bytes(response.content)
    (base / "response_headers.json").write_text(
        json.dumps(dict(response.headers), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (base / "request_headers.json").write_text(
        json.dumps(request_headers, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (base / "request_payload.bin").write_bytes(request_payload)
    (base / "meta.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def log_attempt(
    packet_id: str,
    attempt_num: int,
    method: str,
    status_code: int,
    latency_ms: float,
    delay_ms: int,
    sha256_hex: str,
) -> None:
    """Imprime línea de log para un intento."""
    ts = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    prefix = sha256_hex[:16]
    print(
        f"[{ts}] {packet_id} attempt={attempt_num} method={method} "
        f"status={status_code} latency={latency_ms:.1f}ms delay={delay_ms}ms sha256={prefix}"
    )


def parse_with_parser(
    body: bytes,
    source_tag: str,
) -> dict[str, Any] | None:
    """Parsea el body con el parser correspondiente al source_tag."""
    mod = PARSER_MAP.get(source_tag)
    if mod is None:
        return None
    try:
        result = mod.parse_response(body, source_tag, PARSER_VERSION)
    except Exception:
        return None
    # Si es Indeterminate (tiene "reason"), devolvemos el dict tal cual
    if isinstance(result, dict):
        return result
    # TypedDict se comporta como dict en runtime
    try:
        return dict(result)  # type: ignore[call-overload]
    except Exception:
        return None


def is_indeterminate_result(result: dict[str, Any] | None) -> bool:
    """Verifica si el resultado es Indeterminate (None o tiene 'reason')."""
    if result is None:
        return True
    return "reason" in result


def extract_schema_from_parsed(result: dict[str, Any] | None) -> list[str]:
    """Extrae las claves presentes y no vacías del resultado parseado."""
    if result is None:
        return []
    schema: list[str] = []
    keys = (
        "counts",
        "nominal",
        "metadata",
        "group_sentido",
        "classification",
        "post_evidence",
    )
    for key in keys:
        val = result.get(key)
        if val is not None and val != []:
            schema.append(key)
    return schema


def run_packet(
    packet: dict[str, Any],
    config: dict[str, Any],
    output_dir: Path,
    ua_pool: list[str],
) -> dict[str, Any]:
    """Ejecuta un packet completo: requests, parseo y generación de manifest."""
    packet_id = packet["packet_id"]
    source_tag = packet["source_tag"]
    method = packet.get("method", "GET").upper()
    url = build_url(packet)

    # Delay base
    if "dip" in source_tag:
        delay_cfg = config.get("delay", {}).get("diputados", {})
    else:
        delay_cfg = config.get("delay", {}).get("senado", {})
    base_ms = delay_cfg.get("base_ms", 500)
    jitter_ms = delay_cfg.get("jitter_ms", 300)

    # Repeticiones
    repetitions = get_repetitions(packet, config)

    # Timeout
    timeout_key = get_timeout_key(url, source_tag, packet_id)
    timeout_val = config.get("timeout_seconds", {}).get(timeout_key, 20.0)
    timeout = httpx.Timeout(timeout_val)

    # Headers base
    base_headers: dict[str, str] = {
        "User-Agent": random.choice(ua_pool),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-MX,es;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }
    packet_headers = packet.get("headers")
    if isinstance(packet_headers, dict):
        base_headers.update({k: str(v) for k, v in packet_headers.items()})

    # Payload
    parameters = packet.get("parameters") or {}
    request_payload_bytes = b""
    if method == "POST" and parameters:
        request_payload_bytes = urllib.parse.urlencode(parameters).encode("utf-8")
        base_headers.setdefault("Content-Type", "application/x-www-form-urlencoded")

    # Backoff config
    backoff_cfg = config.get("backoff", {})
    backoff_base = backoff_cfg.get("base_ms", 1000)
    backoff_multiplier = backoff_cfg.get("multiplier", 2.0)
    backoff_max = backoff_cfg.get("max_ms", 16000)

    # Ejecución
    attempt_results: list[dict[str, Any]] = []
    waf_detected = False
    indeterminate_reason: str | None = None
    cache_detected = False

    with httpx.Client(timeout=timeout, follow_redirects=True) as client:
        for attempt_num in range(1, repetitions + 1):
            delay = base_ms + random.randint(0, jitter_ms)
            if attempt_num > 1:
                extra = min(backoff_base * (backoff_multiplier ** (attempt_num - 2)), backoff_max)
                delay += int(extra)

            time.sleep(delay / 1000.0)

            req_headers = dict(base_headers)
            # Rotar UA por attempt
            req_headers["User-Agent"] = random.choice(ua_pool)

            start = time.perf_counter()
            try:
                if method == "POST":
                    response = client.post(url, headers=req_headers, content=request_payload_bytes)
                else:
                    response = client.get(url, headers=req_headers)
                status_code = response.status_code
            except httpx.TimeoutException:
                status_code = 0
                latency_ms = (time.perf_counter() - start) * 1000.0
                # Construimos un objeto falso para unificar flujo
                response = httpx.Response(
                    0,
                    content=b"",
                    headers=httpx.Headers(),
                    request=httpx.Request(method, url),
                )
            except Exception as exc:
                status_code = 0
                latency_ms = (time.perf_counter() - start) * 1000.0
                response = httpx.Response(
                    0,
                    content=str(exc).encode("utf-8", errors="replace"),
                    headers=httpx.Headers(),
                    request=httpx.Request(method, url),
                )
            else:
                latency_ms = (time.perf_counter() - start) * 1000.0

            sha256_hex = hashlib.sha256(response.content).hexdigest()

            log_attempt(
                packet_id=packet_id,
                attempt_num=attempt_num,
                method=method,
                status_code=status_code,
                latency_ms=latency_ms,
                delay_ms=delay,
                sha256_hex=sha256_hex,
            )

            meta = {
                "timestamp_utc": datetime.now(UTC).isoformat(),
                "latency_ms": round(latency_ms, 3),
                "status_code": status_code,
                "probe_id": packet_id,
                "attempt_num": attempt_num,
                "url": str(response.request.url) if response.request else url,
                "method": method,
                "sha256_response": sha256_hex,
            }

            save_attempt(
                output_dir=output_dir,
                packet_id=packet_id,
                attempt_num=attempt_num,
                response=response,
                request_headers=req_headers,
                request_payload=request_payload_bytes,
                meta=meta,
            )

            waf_flag, waf_reason = check_waf_from_response(response, packet, config)
            if waf_flag:
                waf_detected = True
                indeterminate_reason = waf_reason
                attempt_results.append({
                    "attempt": attempt_num,
                    "delay_ms": delay,
                    "status_code": status_code,
                    "waf_detected": True,
                    "sha256": sha256_hex,
                })
                print(
                    f"  -> WAF/Bloqueo detectado en {packet_id} "
                    f"attempt={attempt_num}. Deteniendo repeticiones."
                )
                break

            if check_cache_detected(response, config, source_tag):
                cache_detected = True

            attempt_results.append({
                "attempt": attempt_num,
                "delay_ms": delay,
                "status_code": status_code,
                "waf_detected": False,
                "sha256": sha256_hex,
            })

    # ------------------------------------------------------------------
    # Post-proceso: parsear cada attempt y generar manifest
    # ------------------------------------------------------------------
    hashes_list: list[dict[str, Any]] = []
    parsed_counts_list: list[dict[str, Any]] = []
    retry_history: list[dict[str, Any]] = []
    hash_freq: dict[str, int] = {}
    ok_status_count = 0
    ok_structure_count = 0

    for ar in attempt_results:
        anum = ar["attempt"]
        hashes_list.append({
            "attempt": anum,
            "sha256": ar["sha256"],
            "status_code": ar["status_code"],
        })
        retry_history.append({
            "attempt": anum,
            "delay_ms": ar["delay_ms"],
            "status_code": ar["status_code"],
            "waf_detected": ar["waf_detected"],
        })
        hash_freq[ar["sha256"]] = hash_freq.get(ar["sha256"], 0) + 1
        if ar["status_code"] == 200:
            ok_status_count += 1

        body_path = output_dir / "xraw" / packet_id / f"attempt_{anum}" / "response_body.bin"
        if body_path.exists():
            body = body_path.read_bytes()
            parsed = parse_with_parser(body, source_tag)
            is_indet = is_indeterminate_result(parsed)
            if not is_indet and parsed is not None:
                ok_structure_count += 1

            nominal_count = 0
            if parsed is not None and not is_indet:
                if "nominal" in parsed and isinstance(parsed["nominal"], list):
                    nominal_count = len(parsed["nominal"])
                elif "group_sentido" in parsed and isinstance(parsed["group_sentido"], dict):
                    nominal_count = len(parsed["group_sentido"])

            parsed_counts_list.append({
                "attempt": anum,
                "parsed": not is_indet,
                "counts": parsed if not is_indet else None,
                "nominal_count": nominal_count,
            })
        else:
            parsed_counts_list.append({
                "attempt": anum,
                "parsed": False,
                "counts": None,
                "nominal_count": 0,
            })

    # Thresholds dinámicos proporcionales a repeticiones.
    # Fórmula: max(1, repetitions - 1)
    #   rep=1 -> 1 (un solo attempt exitoso es suficiente)
    #   rep=2 -> 1 (permite 1 fallo, mayoría simple)
    #   rep=3 -> 2 (permite 1 fallo, exige 2 de 3)
    # Los valores en config se mantienen como referencia, pero el runner
    # prioriza el cálculo dinámico para evitar umbrales imposibles.
    thresholds_cfg = config.get("thresholds", {})
    thr_status = max(1, repetitions - 1)
    thr_hash = max(1, repetitions - 1)
    thr_struct = max(1, repetitions - 1)

    max_hash_count = max(hash_freq.values()) if hash_freq else 0

    if waf_detected:
        status = "INDETERMINATE"
        classification = f"INDETERMINATE_{indeterminate_reason or 'WAF_DETECTED'}"
    elif (
        ok_status_count >= thr_status
        and ok_structure_count >= thr_struct
        and max_hash_count >= thr_hash
    ):
        status = "SUCCESS"
        total = len(attempt_results)
        classification = f"SUCCESS_{ok_status_count}of{total}_200_hash_consensus"
    else:
        # Mayoría de attempts Indeterminate del parser?
        indet_parser_count = sum(1 for p in parsed_counts_list if not p["parsed"])
        total = len(attempt_results)
        if indet_parser_count > total // 2:
            status = "INDETERMINATE"
            classification = "INDETERMINATE_PARSER_MAJORITY_INDET"
            indeterminate_reason = indeterminate_reason or "PARSER_MAJORITY_INDET"
        else:
            status = "FAIL"
            classification = (
                f"FAIL_status{ok_status_count}_"
                f"struct{ok_structure_count}_hash{max_hash_count}"
            )

    # variance_from_expected
    expected_schema = packet.get("expected_schema", [])
    parsed_schema_found: list[str] = []
    for p in parsed_counts_list:
        if p["parsed"] and p["counts"] is not None:
            parsed_schema_found = extract_schema_from_parsed(p["counts"])
            if parsed_schema_found:
                break

    manifest = {
        "packet_id": packet_id,
        "source_tag": source_tag,
        "status": status,
        "attempts": len(attempt_results),
        "hashes": hashes_list,
        "parsed_counts": parsed_counts_list,
        "variance_from_expected": {
            "expected_schema": expected_schema,
            "parsed_schema_found": parsed_schema_found,
            "match": set(parsed_schema_found) == set(expected_schema),
        },
        "retry_history": retry_history,
        "waf_detected": waf_detected,
        "cache_detected": cache_detected,
        "indeterminate_reason": indeterminate_reason,
        "thresholds_applied": {
            "status_success": thr_status,
            "hash_consensus": thr_hash,
            "structure_ok": thr_struct,
        },
        "classification": classification,
    }

    manifest_dir = output_dir / "xmanifest"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = manifest_dir / f"XM-{packet_id}.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"  -> Manifest generado: {manifest_path}")

    return manifest


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main(argv: list[str] | None = None) -> int:
    """Punto de entrada CLI del P0 Runner."""
    parser = argparse.ArgumentParser(description="P0 Runner — Validación empírica controlada F1")
    parser.add_argument(
        "--packet-id",
        nargs="+",
        default=None,
        help="Filtrar por uno o varios packet IDs",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path.cwd(),
        help="Directorio de salida para xraw/ y xmanifest/ (default: CWD)",
    )
    args = parser.parse_args(argv)

    if not CONFIG_PATH.exists():
        print(f"ERROR: No se encontró config en {CONFIG_PATH}", file=sys.stderr)
        return 1

    config = load_config(CONFIG_PATH)
    ua_pool = config.get("user_agent_pool", [])
    if not ua_pool:
        print("ERROR: user_agent_pool vacío en config", file=sys.stderr)
        return 1

    filter_ids = args.packet_id if args.packet_id else None
    packets = discover_packets(PACKETS_DIR, filter_ids=filter_ids)
    if not packets:
        print("WARNING: No se encontraron packets para ejecutar.", file=sys.stderr)
        return 0

    print(f"P0 Runner iniciado. Packets a ejecutar: {len(packets)}")
    for pkt in packets:
        print(f"  - {pkt['packet_id']} ({pkt['source_tag']})")

    for pkt in packets:
        print(f"\n>>> Ejecutando {pkt['packet_id']} ...")
        try:
            run_packet(pkt, config, args.output_dir, ua_pool)
        except Exception as exc:
            print(f"ERROR ejecutando {pkt['packet_id']}: {exc}", file=sys.stderr)
            # Continuamos con el siguiente packet

    print("\nP0 Runner finalizado.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
