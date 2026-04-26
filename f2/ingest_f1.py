#!/usr/bin/env python3
"""Ingestión idempotente de manifests Fase 1 al schema productivo SQLite.

Uso:
    python f2/ingest_f1.py [--manifest-dir DIR] [--raw-dir DIR] [--db-path PATH] [--dry-run]
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

from pydantic import ValidationError

# Asegurar que la raíz del proyecto esté en PYTHONPATH para imports relativos
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from f1.parsers.xp_utils import _fix_mojibake_name
from f2.models import (
    AssetRole,
    IngestionReport,
    Legislature,
    Method,
    RawVoteCast,
    RawVoteEvent,
    Sentido,
    SourceAsset,
    SourceTag,
    VoteCounts,
    VoteEventAsset,
)
from shared.persistence_core import (
    insert_raw_vote_casts,
    insert_raw_vote_event,
    insert_source_asset,
    insert_vote_counts,
    insert_vote_event_asset,
)
from shared.transform_bridge import (
    build_counts,
    infer_chamber,
    map_source_tag,
    normalize_sentido,
    parse_date_heuristic,
    validate_counts_vs_nominal,
)


# ---------------------------------------------------------------------------
# Helpers generales
# ---------------------------------------------------------------------------
# Helpers generales
# ---------------------------------------------------------------------------
def resolve_project_root() -> Path:
    """Resuelve la raíz del proyecto asumiendo que este script vive en f2/."""
    return Path(__file__).resolve().parent.parent


def _sha256_file(path: Path) -> str:
    """Calcula sha256 de un archivo binario."""
    h = hashlib.sha256()
    h.update(path.read_bytes())
    return h.hexdigest()


# ---------------------------------------------------------------------------
# Descubrimiento y carga de manifests
# ---------------------------------------------------------------------------
def discover_manifests(manifest_dir: Path) -> list[Path]:
    """Devuelve la lista ordenada de archivos ``XM-*.json``."""
    return sorted(manifest_dir.glob("XM-*.json"))


def load_manifest(path: Path) -> dict:
    """Carga un manifest JSON como ``dict``."""
    return json.loads(path.read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# Lógica de consenso
# ---------------------------------------------------------------------------
def get_consensus_attempt(manifest: dict) -> int | None:
    """Determina el attempt consensuado (sha256 más frecuente; empate → más bajo)."""
    hashes = manifest.get("hashes", [])
    if not hashes:
        return None

    freq: dict[str, list[int]] = {}
    for h in hashes:
        sha = h["sha256"]
        freq.setdefault(sha, []).append(h["attempt"])

    best_sha = None
    best_attempt = None
    max_freq = -1

    for sha, attempts in freq.items():
        count = len(attempts)
        min_attempt = min(attempts)
        if count > max_freq or (count == max_freq and min_attempt < best_attempt):
            max_freq = count
            best_sha = sha
            best_attempt = min_attempt

    return best_attempt


def get_parsed_for_attempt(manifest: dict, attempt_num: int) -> dict | None:
    """Extrae el dict de datos parseados para un attempt dado."""
    for pc in manifest.get("parsed_counts", []):
        if pc.get("attempt") == attempt_num and pc.get("parsed"):
            return pc.get("counts") or {}
    return None


# ---------------------------------------------------------------------------
# Mapeos de inferencia
# ---------------------------------------------------------------------------
def infer_legislature(_source_tag: str) -> Legislature:
    """Todos los packets actuales de F1 son LXVI."""
    return Legislature.LXVI


def infer_asset_role(source_tag: str, parsed: dict | None) -> AssetRole:
    """Infier el rol del asset en función del tipo de fuente y datos disponibles."""
    nominal = parsed.get("nominal") if parsed else None
    group_sentido = parsed.get("group_sentido") if parsed else None
    counts = parsed.get("counts") if parsed else None

    if source_tag == SourceTag.DIP_SITL:
        if nominal and len(nominal) > 0:
            return AssetRole.PRIMARY_NOMINAL
        if counts:
            return AssetRole.PRIMARY_AGGREGATE
        return AssetRole.METADATA

    if source_tag == SourceTag.SEN_LXVI_AJAX:
        if nominal and len(nominal) > 0:
            return AssetRole.PRIMARY_NOMINAL
        return AssetRole.METADATA

    if source_tag == SourceTag.DIP_GACETA_TABLA:
        if group_sentido:
            return AssetRole.PRIMARY_AGGREGATE
        return AssetRole.METADATA

    if source_tag == SourceTag.DIP_GACETA_POST:
        return AssetRole.METADATA

    if source_tag == SourceTag.SEN_LXVI_HTML:
        return AssetRole.TRIANGULATION

    return AssetRole.METADATA


# ---------------------------------------------------------------------------
# Extracción de modelos
# ---------------------------------------------------------------------------
def extract_asset_from_manifest(
    manifest: dict,
    attempt_num: int,
    raw_dir: Path,
    project_root: Path,
) -> SourceAsset:
    """Construye un ``SourceAsset`` a partir del manifest y archivos xraw."""
    packet_id = manifest["packet_id"]
    source_tag = map_source_tag(manifest["source_tag"], packet_id)

    attempt_dir = raw_dir / packet_id / f"attempt_{attempt_num}"
    meta_path = attempt_dir / "meta.json"
    body_path = attempt_dir / "response_body.bin"
    headers_path = attempt_dir / "response_headers.json"

    if not meta_path.exists():
        raise FileNotFoundError(f"meta.json no encontrado: {meta_path}")
    if not body_path.exists():
        raise FileNotFoundError(f"response_body.bin no encontrado: {body_path}")

    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    body_hash = _sha256_file(body_path)

    headers_hash = None
    content_type = None
    if headers_path.exists():
        headers_hash = _sha256_file(headers_path)
        headers = json.loads(headers_path.read_text(encoding="utf-8"))
        content_type = headers.get("content-type") or headers.get("Content-Type")

    # Intentar obtener encoding de los metadatos parseados
    parsed_data = get_parsed_for_attempt(manifest, attempt_num)
    encoding = None
    if parsed_data and isinstance(parsed_data, dict):
        meta_parsed = parsed_data.get("meta", {})
        if isinstance(meta_parsed, dict):
            encoding = meta_parsed.get("encoding")
        if not encoding:
            encoding = parsed_data.get("encoding")

    if not encoding and content_type:
        m = re.search(r"charset=([^\s;]+)", content_type, re.IGNORECASE)
        if m:
            encoding = m.group(1).strip('"\'')

    # Ruta relativa al proyecto si es posible
    try:
        raw_body_path = body_path.relative_to(project_root)
    except ValueError:
        raw_body_path = body_path

    return SourceAsset(
        source_tag=source_tag,
        url=meta["url"],
        method=Method(meta.get("method", "GET").upper()),
        response_body_hash=body_hash,
        response_headers_hash=headers_hash,
        status_code=meta.get("status_code"),
        content_type=content_type,
        encoding=encoding,
        captured_at=datetime.fromisoformat(meta["timestamp_utc"]),
        waf_detected=manifest.get("waf_detected", False),
        cache_detected=manifest.get("cache_detected", False),
        repetition_num=len(manifest.get("hashes", [])),
        run_id=None,
        raw_body_path=raw_body_path,
    )


def extract_vote_event(
    manifest: dict,
    asset_url: str,
    source_tag: str,
) -> RawVoteEvent:
    """Construye un ``RawVoteEvent`` a partir del manifest."""
    chamber = infer_chamber(source_tag)
    legislature = infer_legislature(source_tag)

    consensus = get_consensus_attempt(manifest)
    parsed = get_parsed_for_attempt(manifest, consensus) if consensus else None
    metadata: dict = {}
    if parsed and isinstance(parsed, dict):
        metadata = parsed.get("metadata", {}) or {}
        if not isinstance(metadata, dict):
            metadata = {}

    vote_date = None
    title = None
    subject = None

    if metadata:
        vote_date = parse_date_heuristic(
            metadata.get("fecha") or metadata.get("date") or metadata.get("vote_date")
        )
        title = metadata.get("titulo") or metadata.get("title") or metadata.get("nomtit")
        subject = metadata.get("asunto") or metadata.get("subject")

    # Fallback a meta interno del parser
    if parsed and isinstance(parsed, dict):
        meta = parsed.get("meta", {})
        if isinstance(meta, dict):
            if not title:
                title = meta.get("nomtit") or meta.get("title") or meta.get("titulo")
            if not subject:
                subject = meta.get("asunto") or meta.get("subject")
            if not vote_date:
                vote_date = parse_date_heuristic(meta.get("fecha") or meta.get("date"))

    return RawVoteEvent(
        chamber=chamber,
        legislature=legislature,
        vote_date=vote_date,
        title=title,
        subject=subject,
        source_url=asset_url,
        metadata_json=metadata if metadata else None,
    )


def extract_casts(
    manifest: dict,
    vote_event_id: int,
    asset_id: int,
    source_tag: str,
) -> list[RawVoteCast]:
    """Extrae la lista de ``RawVoteCast`` del attempt consensuado."""
    casts: list[RawVoteCast] = []
    consensus = get_consensus_attempt(manifest)
    parsed = get_parsed_for_attempt(manifest, consensus) if consensus else None
    if not parsed:
        return casts

    nominal = parsed.get("nominal")
    if not nominal:
        return casts

    for row in nominal:
        if not isinstance(row, dict):
            continue

        if source_tag == SourceTag.SEN_LXVI_AJAX:
            name = row.get("nombre", "")
            group = row.get("grupo")
            sentido_raw = row.get("sentido", "")
        elif source_tag == SourceTag.DIP_SITL:
            name = row.get("partidot", "")
            group = None
            sentido_raw = row.get("sentido", "")
        else:
            continue

        if not name:
            continue

        name = _fix_mojibake_name(name)
        if group:
            group = _fix_mojibake_name(group)

        sentido_str = normalize_sentido(sentido_raw, source_tag)
        if sentido_str is None:
            continue
        try:
            sentido = Sentido(sentido_str)
        except ValueError:
            continue

        casts.append(
            RawVoteCast(
                vote_event_id=vote_event_id,
                asset_id=asset_id,
                legislator_name=name,
                legislator_group=group,
                sentido=sentido,
                raw_row_json=row,
            )
        )

    return casts


def extract_counts(
    manifest: dict,
    vote_event_id: int,
    asset_id: int,
) -> list[VoteCounts]:
    """Extrae la lista de ``VoteCounts`` del attempt consensuado."""
    consensus = get_consensus_attempt(manifest)
    parsed = get_parsed_for_attempt(manifest, consensus) if consensus else None
    if not parsed:
        return []

    return [
        VoteCounts(
            vote_event_id=vote_event_id,
            asset_id=asset_id,
            group_name=cd.get("group_name"),
            a_favor=cd.get("a_favor", 0),
            en_contra=cd.get("en_contra", 0),
            abstencion=cd.get("abstencion", 0),
            ausente=cd.get("ausente", 0),
            novoto=cd.get("novoto", 0),
            presente=cd.get("presente", 0),
            total=cd.get("total"),
        )
        for cd in build_counts(parsed)
    ]


# ---------------------------------------------------------------------------
# Ingestión de un manifest individual
# ---------------------------------------------------------------------------
def ingest_manifest(
    conn: sqlite3.Connection,
    manifest: dict,
    raw_dir: Path,
    project_root: Path,
    report: IngestionReport,
    dry_run: bool,
) -> None:
    """Procesa un único manifest y actualiza el reporte."""
    packet_id = manifest.get("packet_id", "UNKNOWN")
    status = manifest.get("status", "")

    if status != "SUCCESS":
        return

    consensus_attempt = get_consensus_attempt(manifest)
    if consensus_attempt is None:
        report.errors.append(f"{packet_id}: no se encontró attempt consensuado")
        return

    parsed_data = get_parsed_for_attempt(manifest, consensus_attempt)
    if not parsed_data:
        report.errors.append(
            f"{packet_id}: parsed_counts no tiene datos para attempt {consensus_attempt}"
        )
        return

    # Validación cruzada counts vs nominal (solo warning en ingestión histórica)
    counts = parsed_data.get("counts")
    nominal = parsed_data.get("nominal")
    if counts is not None and nominal is not None:
        validation = validate_counts_vs_nominal(counts, nominal)
        if not validation.get("ok", True):
            logger.warning(
                "Counts vs nominal mismatch (%s): %s", packet_id, validation
            )
            report.errors.append(
                f"{packet_id}: counts vs nominal mismatch: {validation}"
            )

    # ------------------------------------------------------------------
    # Asset extraction (before transaction)
    # ------------------------------------------------------------------
    try:
        asset = extract_asset_from_manifest(manifest, consensus_attempt, raw_dir, project_root)
    except (FileNotFoundError, ValidationError, Exception) as exc:
        report.errors.append(f"{packet_id}: error extrayendo asset: {exc}")
        return

    source_tag = map_source_tag(manifest["source_tag"], packet_id)

    # ------------------------------------------------------------------
    # Vote event extraction (before transaction)
    # ------------------------------------------------------------------
    try:
        event = extract_vote_event(manifest, str(asset.url), source_tag)
    except ValidationError as exc:
        report.errors.append(f"{packet_id}: error validando vote_event: {exc}")
        return

    # ------------------------------------------------------------------
    # Atomic transaction for all DB mutations
    # ------------------------------------------------------------------
    if conn is not None and not dry_run:
        conn.execute("BEGIN")

    try:
        # --- Asset ---
        asset_id: int | None = None
        asset_inserted = False
        if dry_run:
            report.assets_skipped += 1
        else:
            asset_id, asset_inserted = insert_source_asset(conn, asset.model_dump(mode="json"))
            if asset_inserted:
                report.assets_inserted += 1
            else:
                report.assets_skipped += 1

        # --- Vote event ---
        vote_event_id: int | None = None
        event_inserted = False
        if dry_run:
            report.vote_events_linked += 1
        else:
            vote_event_id, event_inserted = insert_raw_vote_event(conn, event.model_dump(mode="json"))
            if event_inserted:
                report.vote_events_inserted += 1
            report.vote_events_linked += 1

        # --- Vote event asset ---
        if not dry_run and asset_id is not None and vote_event_id is not None:
            link = VoteEventAsset(
                vote_event_id=vote_event_id,
                asset_id=asset_id,
                asset_role=infer_asset_role(source_tag, parsed_data),
            )
            insert_vote_event_asset(conn, link.model_dump(mode="json"))

        # --- Casts & Counts extraction ---
        _ve_id = vote_event_id if vote_event_id is not None else 0
        _asset_id = asset_id if asset_id is not None else 0
        casts = extract_casts(manifest, _ve_id, _asset_id, source_tag)
        counts_list = extract_counts(manifest, _ve_id, _asset_id)

        # --- Casts insert ---
        if casts and not dry_run and asset_id is not None and vote_event_id is not None:
            casts_inserted = insert_raw_vote_casts(conn, [c.model_dump(mode="json") for c in casts])
            report.casts_inserted += casts_inserted

        # --- Counts insert ---
        if counts_list and not dry_run and asset_id is not None and vote_event_id is not None:
            counts_inserted = insert_vote_counts(conn, [c.model_dump(mode="json") for c in counts_list])
            report.counts_inserted += counts_inserted

        if conn is not None and not dry_run:
            conn.commit()
    except Exception as exc:
        if conn is not None and not dry_run:
            conn.rollback()
        report.errors.append(f"{packet_id}: error durante ingestión: {exc}")
        raise

    # ------------------------------------------------------------------
    # Log de progreso
    # ------------------------------------------------------------------
    print(
        f"[{packet_id}] status=SUCCESS attempts={len(manifest.get('hashes', []))} "
        f"consensus=attempt_{consensus_attempt} asset_id={asset_id} "
        f"vote_event_id={vote_event_id} casts={len(casts)} counts={len(counts_list)}"
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Ingestión idempotente de manifests Fase 1 al schema SQLite productivo."
    )
    parser.add_argument(
        "--manifest-dir",
        type=Path,
        default=resolve_project_root() / "xmanifest",
        help="Directorio con manifests XM-*.json (default: xmanifest/)",
    )
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=resolve_project_root() / "xraw",
        help="Directorio con datos crudos xraw/ (default: xraw/)",
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=resolve_project_root() / "data" / "historico.db",
        help="Ruta a la base de datos SQLite (default: data/historico.db)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Solo valida, no inserta en la base de datos.",
    )
    args = parser.parse_args(argv)

    project_root = resolve_project_root()
    manifest_dir: Path = args.manifest_dir
    raw_dir: Path = args.raw_dir
    db_path: Path = args.db_path.resolve()
    dry_run: bool = args.dry_run

    if not manifest_dir.exists():
        print(f"❌ Directorio de manifests no existe: {manifest_dir}", file=sys.stderr)
        return 1
    if not raw_dir.exists():
        print(f"❌ Directorio de raw no existe: {raw_dir}", file=sys.stderr)
        return 1

    # Conexión a SQLite (solo para validar en dry-run; en modo real se requiere DB)
    conn: sqlite3.Connection | None = None
    if not dry_run:
        if not db_path.exists():
            print(f"❌ Base de datos no existe: {db_path}", file=sys.stderr)
            print("   Ejecuta primero: python f2/db_init.py", file=sys.stderr)
            return 1
        conn = sqlite3.connect(str(db_path))
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.execute("PRAGMA busy_timeout = 5000;")

    manifests = discover_manifests(manifest_dir)
    print(f"📁 Manifests descubiertos: {len(manifests)}")

    report = IngestionReport(
        assets_inserted=0,
        assets_skipped=0,
        vote_events_inserted=0,
        vote_events_linked=0,
        casts_inserted=0,
        counts_inserted=0,
        manifests_processed=0,
    )

    for mpath in manifests:
        try:
            manifest = load_manifest(mpath)
        except Exception as exc:
            report.errors.append(f"{mpath.name}: error cargando manifest: {exc}")
            continue

        if manifest.get("status") != "SUCCESS":
            continue

        report.manifests_processed += 1
        try:
            ingest_manifest(
                conn=conn,  # type: ignore[arg-type]
                manifest=manifest,
                raw_dir=raw_dir,
                project_root=project_root,
                report=report,
                dry_run=dry_run,
            )
        except Exception:
            continue

    if conn is not None:
        conn.close()

    print("\n📊 IngestionReport:")
    print(report.model_dump_json(indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
