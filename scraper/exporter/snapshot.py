"""Orquestación de exportación de snapshots para popolo-congreso-union v0.1."""

from __future__ import annotations

import logging
import secrets
import shutil
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scraper.exporter.artifacts import (
    build_cache_index,
    build_manifest,
    build_provenance,
    build_quality_report,
    write_artifacts,
)
from scraper.exporter.mapping import source_chamber_to_contract_camara
from scraper.exporter.raw_db import create_raw_db

logger = logging.getLogger(__name__)

_VALID_LEGISLATURES = {"LXIV", "LXV", "LXVI"}


def export_snapshot(
    db_path: Path,
    raw_dir: Path,
    output_base: Path,
    chamber_source: str,  # 'diputados'
    legislature: str,  # 'LXVI'
) -> dict[str, Any]:
    """Exporta un snapshot completo para una cámara y legislatura.

    Genera:
    - manifest.json
    - cache_index.json
    - raw.db
    - quality_report.json
    - provenance.json
    - Copia de payloads crudos referenciados

    Args:
        db_path: Path al source DB (historico.db).
        raw_dir: Directorio con payloads crudos (data/raw/).
        output_base: Directorio base para snapshots.
        chamber_source: 'diputados' o 'senado'.
        legislature: 'LXIV', 'LXV', 'LXVI'.

    Returns:
        Dict con:
        - "package_id": ID del paquete
        - "snapshot_dir": Path al directorio del snapshot
        - "row_counts": Dict de tabla → filas insertadas en raw.db
        - "artifacts": Dict de artifact_name → filename

    Raises:
        FileExistsError: Si el snapshot ya existe (idempotencia).
        ValueError: Si chamber no es soportada o legislatura fuera de scope.
    """
    # ------------------------------------------------------------------
    # 1. Validar scope
    # ------------------------------------------------------------------
    if chamber_source != "diputados":
        raise ValueError(
            f"Chamber no soportado: {chamber_source!r}. "
            f"Solo 'diputados' está habilitado en este contrato."
        )
    if legislature not in _VALID_LEGISLATURES:
        raise ValueError(
            f"Legislatura fuera de scope: {legislature!r}. "
            f"Valores válidos: {sorted(_VALID_LEGISLATURES)}"
        )

    # ------------------------------------------------------------------
    # 2. Generar package_id y run_id
    # ------------------------------------------------------------------
    camara_code = source_chamber_to_contract_camara(chamber_source)
    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    run_id = f"{timestamp}_{secrets.token_hex(4)}"
    package_id = f"snapshot_{camara_code}_{legislature}_{timestamp}"

    # ------------------------------------------------------------------
    # 3. Crear directorio snapshot
    # ------------------------------------------------------------------
    snapshot_dir = output_base / package_id
    if snapshot_dir.exists():
        raise FileExistsError(
            f"Snapshot ya existe: {snapshot_dir}. "
            f"Operación abortada por idempotencia."
        )
    snapshot_dir.mkdir(parents=True, exist_ok=False)

    logger.info("Snapshot dir creado: %s", snapshot_dir)

    # ------------------------------------------------------------------
    # 4. Conectar al source DB
    # ------------------------------------------------------------------
    source_conn = sqlite3.connect(str(db_path))

    try:
        # ------------------------------------------------------------------
        # 5. Generar raw.db
        # ------------------------------------------------------------------
        raw_db_path = snapshot_dir / "raw.db"
        logger.info("Generando raw.db en %s ...", raw_db_path)
        row_counts = create_raw_db(
            raw_db_path, source_conn, chamber_source, legislature, package_id, run_id
        )
        logger.info("raw.db poblado: %s", row_counts)

        # ------------------------------------------------------------------
        # 6. Generar artefactos JSON
        # ------------------------------------------------------------------
        camara = source_chamber_to_contract_camara(chamber_source)

        cache_index = build_cache_index(
            source_conn, camara, legislature, chamber_source
        )
        quality_report = build_quality_report(camara, legislature)
        provenance = build_provenance(
            source_conn, camara, legislature, chamber_source
        )

        # ------------------------------------------------------------------
        # 7. Copiar payloads crudos
        # ------------------------------------------------------------------
        payload_count = _copy_payloads(
            source_conn, raw_dir, snapshot_dir, chamber_source, legislature, cache_index
        )
        logger.info("Payloads copiados: %d", payload_count)

        # ------------------------------------------------------------------
        # 8. Construir manifest
        # ------------------------------------------------------------------
        artifact_files: dict[str, str] = {
            "cache_index": "cache_index.json",
            "quality_report": "quality_report.json",
            "provenance": "provenance.json",
            "raw_db": "raw.db",
        }
        if payload_count > 0:
            artifact_files["payloads"] = "payloads/"

        manifest = build_manifest(
            package_id=package_id,
            source_package_id=run_id,
            camara=camara,
            legislatura=legislature,
            artifact_files=artifact_files,
            notes="Diagnostic export; person disambiguation pending.",
        )

        # ------------------------------------------------------------------
        # 9. Escribir artefactos
        # ------------------------------------------------------------------
        written = write_artifacts(
            snapshot_dir, manifest, cache_index, quality_report, provenance
        )
        logger.info("Artefactos escritos: %s", list(written.keys()))

        # ------------------------------------------------------------------
        # 10. Retornar resultado
        # ------------------------------------------------------------------
        return {
            "package_id": package_id,
            "snapshot_dir": str(snapshot_dir),
            "row_counts": row_counts,
            "artifacts": {**written, "raw_db": "raw.db"},
        }

    except BaseException:
        # Limpiar snapshot_dir parcial en caso de error
        logger.exception("Error durante export; limpiando %s", snapshot_dir)
        shutil.rmtree(snapshot_dir, ignore_errors=True)
        raise
    finally:
        source_conn.close()


def _copy_payloads(
    source_conn: sqlite3.Connection,
    raw_dir: Path,
    snapshot_dir: Path,
    chamber_source: str,
    legislature: str,
    cache_index: list[dict[str, Any]],
) -> int:
    """Copia payloads crudos al snapshot y actualiza cache_index refs.

    Para cada asset referenciado en cache_index con payload_ref, copia el
    directorio ``raw_dir/<asset_id>`` a ``snapshot_dir/payloads/<asset_id>``.
    Luego actualiza payload_ref en cache_index al path relativo dentro del
    snapshot.

    Returns:
        Número de directorios de payload copiados.
    """
    # Obtener asset_ids y raw_body_paths desde el source DB
    asset_rows = source_conn.execute(
        """
        SELECT DISTINCT sa.asset_id, sa.raw_body_path
        FROM source_asset sa
        JOIN vote_event_asset vea ON vea.asset_id = sa.asset_id
        JOIN raw_vote_event rve ON rve.vote_event_id = vea.vote_event_id
        WHERE rve.chamber = ? AND rve.legislature = ?
        """,
        (chamber_source, legislature),
    ).fetchall()

    # Mapear asset_id → raw_body_path
    asset_to_raw_path: dict[str, str] = {}
    for asset_id, raw_body_path in asset_rows:
        if raw_body_path is not None:
            asset_to_raw_path[str(asset_id)] = raw_body_path

    if not asset_to_raw_path:
        return 0

    payloads_dir = snapshot_dir / "payloads"
    payloads_dir.mkdir(parents=True, exist_ok=True)

    copied = 0
    # Mapeo: raw_body_path original → path relativo dentro del snapshot
    path_remap: dict[str, str] = {}

    for asset_id_str in asset_to_raw_path:
        src_dir = raw_dir / asset_id_str
        if not src_dir.is_dir():
            logger.debug("Payload source no encontrado, saltando: %s", src_dir)
            continue

        dst_dir = payloads_dir / asset_id_str
        shutil.copytree(src_dir, dst_dir)
        # Registrar el mapeo: raw_body_path original → path relativo nuevo
        original_raw_path = asset_to_raw_path[asset_id_str]
        path_remap[original_raw_path] = f"payloads/{asset_id_str}"
        copied += 1

    # Actualizar payload_ref en cache_index
    for row in cache_index:
        original_ref = row.get("payload_ref")
        if original_ref is not None and original_ref in path_remap:
            row["payload_ref"] = path_remap[original_ref]

    return copied
