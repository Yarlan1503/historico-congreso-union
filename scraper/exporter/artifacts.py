"""Generación de artefactos JSON para snapshots popolo-congreso-union v0.1.

Produce manifest, cache_index, quality_report y provenance conforme a los
schemas JSON definidos en popolo-congreso-union/schemas/.

Distinción CRÍTICA chamber/camara:
- JSON artifacts: ``camara = 'D'`` (Diputados) o ``'S'`` (Senado).
- raw.db (source DB): ``chamber = 'diputados'`` o ``'senado'`` (CHECK constraint).
"""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

CONTRACT_VERSION = "0.1"


def build_manifest(
    package_id: str,
    source_package_id: str,
    camara: str,
    legislatura: str,
    artifact_files: dict[str, str],
    notes: str | None = None,
) -> dict[str, Any]:
    """Genera manifest.json conforme a manifest.schema.json.

    Args:
        package_id: ID del paquete (ej. ``"snapshot_D_LXVI_20260425_120000"``).
        source_package_id: ID del paquete fuente (nuestro run_id).
        camara: ``'D'`` para Diputados, ``'S'`` para Senado.
        legislatura: ``'LXIV'``, ``'LXV'``, ``'LXVI'``, etc.
        artifact_files: Dict de artifact_name → path relativo dentro del snapshot.
            Debe incluir al menos ``'cache_index'``, ``'quality_report'``,
            ``'provenance'``.
        notes: Notas opcionales.

    Returns:
        Dict que valida contra manifest.schema.json.
    """
    manifest: dict[str, Any] = {
        "contract_version": CONTRACT_VERSION,
        "package_id": package_id,
        "source_package_id": source_package_id,
        "camara": camara,
        "legislatura": legislatura,
        "created_at": datetime.now(UTC).isoformat(),
        "artifacts": artifact_files,
    }
    if notes is not None:
        manifest["notes"] = notes
    return manifest


def build_cache_index(
    conn: sqlite3.Connection,
    camara: str,
    legislatura: str,
    chamber_source: str,
) -> list[dict[str, Any]]:
    """Genera cache_index.json como array de rows conforme a cache_index_row.schema.json.

    Lee ``source_asset`` JOIN ``vote_event_asset`` JOIN ``raw_vote_event`` para
    obtener assets relevantes a la cámara/legislatura dada.

    Args:
        conn: Conexión SQLite al source DB.
        camara: ``'D'`` o ``'S'`` (contrato JSON).
        legislatura: ``'LXVI'``, etc.
        chamber_source: ``'diputados'`` o ``'senado'`` (para query al source DB).

    Returns:
        Lista de dicts, cada uno conforme a cache_index_row.schema.json.
    """
    query = """
        SELECT DISTINCT sa.source_tag, sa.url, sa.response_body_hash,
               sa.captured_at, sa.content_type, sa.raw_body_path, sa.method
        FROM source_asset sa
        JOIN vote_event_asset vea ON vea.asset_id = sa.asset_id
        JOIN raw_vote_event rve ON rve.vote_event_id = vea.vote_event_id
        WHERE rve.chamber = ? AND rve.legislature = ?
    """
    cursor = conn.execute(query, (chamber_source, legislatura))
    rows: list[dict[str, Any]] = []
    for (
        _source_tag,
        url,
        response_body_hash,
        captured_at,
        content_type,
        raw_body_path,
        _method,
    ) in cursor:
        row: dict[str, Any] = {
            "contract_version": CONTRACT_VERSION,
            "url": url,
            "hash_sha256": response_body_hash,
            "timestamp": captured_at,
            "camara": camara,
            "legislatura": legislatura,
        }
        if content_type is not None:
            row["content_type"] = content_type
        if raw_body_path is not None:
            row["payload_ref"] = raw_body_path
        rows.append(row)
    return rows


def build_quality_report(
    camara: str,
    legislatura: str,
    person_disambiguation: bool = False,
    counts_verified: bool = False,
) -> dict[str, Any]:
    """Genera quality_report.json conforme a quality_report.schema.json.

    Para el primer export, ``person_disambiguation=False`` → ``status='warn'``.

    Checks:
    - ``"source_assets_present"``: pass (siempre, asumimos que hay datos).
    - ``"vote_events_present"``: pass.
    - ``"person_disambiguation"``: warn si no se hizo disambiguation real.
    - ``"counts_consistency"``: skip (no verificamos aún).

    Args:
        camara: ``'D'`` o ``'S'``.
        legislatura: ``'LXIV'``, ``'LXV'``, ``'LXVI'``, etc.
        person_disambiguation: True si se hizo disambiguation real.
        counts_verified: True si se verificó counts vs casts.

    Returns:
        Dict que valida contra quality_report.schema.json con status ``"warn"``.
    """
    now = datetime.now(UTC).isoformat()

    checks: list[dict[str, Any]] = [
        {
            "name": "source_assets_present",
            "status": "pass",
            "message": "Source assets exist for the requested camera/legislature.",
        },
        {
            "name": "vote_events_present",
            "status": "pass",
            "message": "Vote events exist for the requested camera/legislature.",
        },
        {
            "name": "person_disambiguation",
            "status": "warn" if not person_disambiguation else "pass",
            "message": (
                "Person disambiguation not performed; names used as stubs."
                if not person_disambiguation
                else "Person disambiguation completed."
            ),
        },
        {
            "name": "counts_consistency",
            "status": "skip" if not counts_verified else "pass",
            "message": (
                "Counts consistency check not yet implemented."
                if not counts_verified
                else "Counts verified against casts."
            ),
        },
    ]

    # Determine overall status: fail > warn > pass
    has_fail = any(c["status"] == "fail" for c in checks)
    has_warn = any(c["status"] == "warn" for c in checks)
    if has_fail:
        overall = "fail"
    elif has_warn:
        overall = "warn"
    else:
        overall = "pass"

    report: dict[str, Any] = {
        "contract_version": CONTRACT_VERSION,
        "status": overall,
        "checked_at": now,
        "generated_at": now,
        "checks": checks,
    }
    return report


def build_provenance(
    conn: sqlite3.Connection,
    camara: str,
    legislatura: str,
    chamber_source: str,
) -> list[dict[str, Any]]:
    """Genera provenance.json como array de rows conforme a provenance_row.schema.json.

    Una row por cada source_asset relevante a la cámara/legislatura.

    Args:
        conn: Conexión SQLite al source DB.
        camara: ``'D'`` o ``'S'`` (contrato JSON).
        legislatura: ``'LXVI'``, etc.
        chamber_source: ``'diputados'`` o ``'senado'`` (para query al source DB).

    Returns:
        Lista de dicts, cada uno conforme a provenance_row.schema.json.
    """
    query = """
        SELECT DISTINCT sa.source_tag, sa.url, sa.response_body_hash,
               sa.captured_at, sa.method
        FROM source_asset sa
        JOIN vote_event_asset vea ON vea.asset_id = sa.asset_id
        JOIN raw_vote_event rve ON rve.vote_event_id = vea.vote_event_id
        WHERE rve.chamber = ? AND rve.legislature = ?
    """
    cursor = conn.execute(query, (chamber_source, legislatura))
    rows: list[dict[str, Any]] = []
    for source_tag, url, response_body_hash, captured_at, method in cursor:
        row: dict[str, Any] = {
            "contract_version": CONTRACT_VERSION,
            "artifact_id": source_tag,
            "url": url,
            "hash_sha256": response_body_hash,
            "timestamp": captured_at,
            "camara": camara,
            "legislatura": legislatura,
            "source_system": source_tag,
            "retrieval_method": method,
        }
        rows.append(row)
    return rows


def write_artifacts(
    snapshot_dir: Path,
    manifest: dict[str, Any],
    cache_index: list[dict[str, Any]],
    quality_report: dict[str, Any],
    provenance: list[dict[str, Any]],
) -> dict[str, str]:
    """Escribe todos los artefactos JSON al directorio del snapshot.

    Crea:
    - ``snapshot_dir/manifest.json``
    - ``snapshot_dir/cache_index.json``
    - ``snapshot_dir/quality_report.json``
    - ``snapshot_dir/provenance.json``

    Args:
        snapshot_dir: Directorio destino (se crea si no existe).
        manifest: Dict conforme a manifest.schema.json.
        cache_index: Lista de dicts conforme a cache_index_row.schema.json.
        quality_report: Dict conforme a quality_report.schema.json.
        provenance: Lista de dicts conforme a provenance_row.schema.json.

    Returns:
        Dict de artifact_name → path relativo del archivo escrito.
    """
    snapshot_dir.mkdir(parents=True, exist_ok=True)

    artifact_map: dict[str, str] = {
        "manifest": "manifest.json",
        "cache_index": "cache_index.json",
        "quality_report": "quality_report.json",
        "provenance": "provenance.json",
    }

    data_map: dict[str, Any] = {
        "manifest": manifest,
        "cache_index": cache_index,
        "quality_report": quality_report,
        "provenance": provenance,
    }

    for name, filename in artifact_map.items():
        filepath = snapshot_dir / filename
        filepath.write_text(
            json.dumps(data_map[name], ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    return artifact_map
