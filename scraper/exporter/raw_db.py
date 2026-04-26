"""Creación y población de raw.db conforme a raw_v0_1.sql.

Lee el schema canónico desde popolo-congreso-union y pobla las tablas
desde el source DB del scraper.
"""

from __future__ import annotations

import os
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

from scraper.exporter.mapping import (
    build_membership_key,
    build_motion_key,
    build_person_key,
    build_source_key,
    build_vote_event_key,
    counts_to_rows,
    map_vote_option,
    normalize_person_name,
)
from scraper.person_normalizer import load_catalog, match_person

# Schema path: env var override o path relativo al proyecto hermano.
# Desde scraper/exporter/ → subir 4 niveles = raíz de Proyectos/.
_DEFAULT_SCHEMA = (
    Path(__file__).resolve().parent.parent.parent.parent
    / "popolo-congreso-union" / "schemas" / "sql" / "raw_v0_1.sql"
)
RAW_V0_1_SCHEMA_PATH = Path(os.environ.get("POPOLO_SCHEMA_PATH", str(_DEFAULT_SCHEMA)))

# Placeholder para session_date cuando vote_date es NULL.
# El CHECK constraint exige length(trim(session_date)) > 0 — "" no pasa.
_UNKNOWN_DATE = "unknown"


def _get_id(conn: sqlite3.Connection, table: str, key_col: str, key_val: str) -> int:
    """Retorna el PK de *table* buscando por *key_col* = *key_val*.

    Helper post-INSERT OR IGNORE para obtener el id recién insertado o existente.
    """
    row = conn.execute(f"SELECT {table}_id FROM {table} WHERE {key_col} = ?", (key_val,)).fetchone()
    if row is None:
        raise ValueError(f"_get_id: no row found in {table} where {key_col}={key_val!r}")
    return row[0]


def create_raw_db(
    output_path: Path,
    source_conn: sqlite3.Connection,
    chamber_source: str,
    legislature: str,
    package_id: str,
    run_id: str,
    catalog_path: Path | None = None,
) -> dict[str, int]:
    """Crea raw.db y lo puebla desde el source DB.

    Args:
        output_path: Path donde crear raw.db.
        source_conn: Conexión al source DB (nuestro historico.db).
        chamber_source: Cámara en formato source ('diputados' o 'senado').
        legislature: Legislatura a exportar ('LXIV', 'LXV', 'LXVI').
        package_id: ID del paquete snapshot.
        run_id: ID de la ejecución de export.
        catalog_path: Path opcional al CSV del catálogo de personas.
            Si existe, se usa para poblar source_person_id y organization_key.

    Returns:
        Dict con conteos de filas insertadas por tabla.
        Ej: ``{"raw_import_batch": 1, "raw_source": 5, "raw_person": 10, ...}``

    Raises:
        FileExistsError: Si output_path ya existe.
        RuntimeError: Si hay violaciones de FK después del poblamiento.
    """
    if output_path.exists():
        raise FileExistsError(f"Output file already exists: {output_path}")

    schema_sql = RAW_V0_1_SCHEMA_PATH.read_text(encoding="utf-8")

    # Cargar catálogo de personas si está disponible.
    catalog: dict = {}
    if catalog_path and catalog_path.exists():
        catalog = load_catalog(catalog_path)

    conn = sqlite3.connect(str(output_path), isolation_level=None)
    counts: dict[str, int] = {}

    try:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.executescript(schema_sql)
        conn.execute("BEGIN")

        # ================================================================
        # 1. raw_import_batch
        # ================================================================
        imported_at = datetime.now(UTC).isoformat()
        conn.execute(
            "INSERT OR IGNORE INTO raw_import_batch "
            "(batch_key, contract_version, package_id, imported_at, notes) "
            "VALUES (?, ?, ?, ?, ?)",
            (run_id, "0.1", package_id, imported_at, None),
        )
        batch_id = _get_id(conn, "raw_import_batch", "batch_key", run_id)
        counts["raw_import_batch"] = 1

        # ================================================================
        # 2. raw_source (desde source_asset via vote_event_asset)
        # ================================================================
        source_rows = source_conn.execute(
            """
            SELECT DISTINCT sa.source_tag, sa.url, sa.response_body_hash,
                   sa.captured_at, sa.content_type, sa.raw_body_path
            FROM source_asset sa
            JOIN vote_event_asset vea ON vea.asset_id = sa.asset_id
            JOIN raw_vote_event rve ON rve.vote_event_id = vea.vote_event_id
            WHERE rve.chamber = ? AND rve.legislature = ?
            """,
            (chamber_source, legislature),
        ).fetchall()

        for source_tag, url, body_hash, captured_at, content_type, raw_body_path in source_rows:
            source_key = build_source_key(source_tag, url, body_hash)
            conn.execute(
                "INSERT OR IGNORE INTO raw_source "
                "(raw_import_batch_id, source_key, source_url, payload_ref, "
                "content_type, retrieved_at, hash_sha256) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    batch_id,
                    source_key,
                    url,
                    raw_body_path,
                    content_type,
                    captured_at,
                    body_hash,
                ),
            )
        counts["raw_source"] = len(source_rows)

        # ================================================================
        # 3. raw_person (stubs desde legislator_name)
        # ================================================================
        person_names = source_conn.execute(
            """
            SELECT DISTINCT rvc.legislator_name
            FROM raw_vote_cast rvc
            JOIN raw_vote_event rve ON rve.vote_event_id = rvc.vote_event_id
            WHERE rve.chamber = ? AND rve.legislature = ?
            """,
            (chamber_source, legislature),
        ).fetchall()

        person_key_to_id: dict[str, int] = {}
        for (legislator_name,) in person_names:
            pkey = build_person_key(legislator_name)
            full_name = normalize_person_name(legislator_name)

            # Matching contra catálogo para source_person_id.
            source_person_id = None
            person_notes = "Person stub from legislator_name; no disambiguation performed."
            if catalog:
                pm = match_person(legislator_name, catalog)
                if pm.person_id:
                    source_person_id = pm.person_id
                    person_notes = f"Matched via {pm.method} (confidence={pm.confidence:.2f})."

            conn.execute(
                "INSERT OR IGNORE INTO raw_person "
                "(person_key, full_name, given_name, family_name, "
                "source_person_id, notes) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (pkey, full_name, None, None, source_person_id, person_notes),
            )
            person_key_to_id[pkey] = _get_id(conn, "raw_person", "person_key", pkey)
        counts["raw_person"] = len(person_key_to_id)

        # ================================================================
        # 4. raw_membership (stubs: person + chamber + legislature)
        # ================================================================
        membership_key_to_id: dict[str, int] = {}
        for pkey, person_id in person_key_to_id.items():
            # Buscar party del catálogo para organization_key.
            org_key = None
            if catalog:
                entry = catalog.get(pkey) or catalog.get(pkey.replace(",", ""))
                if entry and entry.party_senado:
                    org_key = entry.party_senado

            mkey = build_membership_key(pkey, chamber_source, legislature)
            conn.execute(
                "INSERT OR IGNORE INTO raw_membership "
                "(membership_key, raw_person_id, raw_post_id, chamber, legislature, "
                "organization_key, start_date, end_date, source_member_id, notes) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    mkey,
                    person_id,
                    None,
                    chamber_source,
                    legislature,
                    org_key,
                    None,
                    None,
                    None,
                    "Membership stub from person_key + chamber + legislature.",
                ),
            )
            membership_key_to_id[mkey] = _get_id(conn, "raw_membership", "membership_key", mkey)
        counts["raw_membership"] = len(membership_key_to_id)

        # ================================================================
        # 5. raw_motion + 6. raw_vote_event (1:1 por source VE)
        # ================================================================
        ve_source_rows = source_conn.execute(
            """
            SELECT vote_event_id, vote_date, title, subject, source_url
            FROM raw_vote_event
            WHERE chamber = ? AND legislature = ?
            """,
            (chamber_source, legislature),
        ).fetchall()

        source_ve_to_target: dict[int, int] = {}
        for ve_id, vote_date, title, subject, source_url in ve_source_rows:
            # Fallback para source_url NULL (evita colisiones de key)
            src_url = source_url if source_url else f"__internal_{ve_id}"

            # --- raw_motion ---
            mkey = build_motion_key(chamber_source, legislature, src_url)
            motion_title = title if title and title.strip() else "Untitled motion"
            conn.execute(
                "INSERT OR IGNORE INTO raw_motion "
                "(motion_key, chamber, legislature, title, description, "
                "source_motion_id) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (mkey, chamber_source, legislature, motion_title, subject, None),
            )
            motion_id = _get_id(conn, "raw_motion", "motion_key", mkey)

            # --- raw_vote_event ---
            ve_key = build_vote_event_key(chamber_source, legislature, src_url)
            session_date = vote_date if vote_date and vote_date.strip() else _UNKNOWN_DATE
            conn.execute(
                "INSERT OR IGNORE INTO raw_vote_event "
                "(vote_event_key, raw_motion_id, chamber, legislature, "
                "session_date, result, source_vote_event_id) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    ve_key,
                    motion_id,
                    chamber_source,
                    legislature,
                    session_date,
                    None,
                    str(ve_id),
                ),
            )
            source_ve_to_target[ve_id] = _get_id(conn, "raw_vote_event", "vote_event_key", ve_key)

        counts["raw_motion"] = len(ve_source_rows)
        counts["raw_vote_event"] = len(source_ve_to_target)

        # ================================================================
        # 7. raw_count (normalizados desde vote_counts, solo > 0)
        # ================================================================
        count_rows = source_conn.execute(
            """
            SELECT vc.vote_event_id, vc.group_name,
                   vc.a_favor, vc.en_contra, vc.abstencion,
                   vc.ausente, vc.novoto, vc.presente
            FROM vote_counts vc
            JOIN raw_vote_event rve ON rve.vote_event_id = vc.vote_event_id
            WHERE rve.chamber = ? AND rve.legislature = ?
            """,
            (chamber_source, legislature),
        ).fetchall()

        n_counts = 0
        for (
            src_ve_id,
            group_name,
            a_favor,
            en_contra,
            abstencion,
            ausente,
            novoto,
            presente,
        ) in count_rows:
            target_ve_id = source_ve_to_target.get(src_ve_id)
            if target_ve_id is None:
                continue
            count_dict = {
                "a_favor": a_favor,
                "en_contra": en_contra,
                "abstencion": abstencion,
                "ausente": ausente,
                "novoto": novoto,
                "presente": presente,
            }
            for count_row in counts_to_rows(count_dict):
                conn.execute(
                    "INSERT OR IGNORE INTO raw_count "
                    "(raw_vote_event_id, option, count_value, count_source, "
                    "group_key, notes) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        target_ve_id,
                        count_row["option"],
                        count_row["count_value"],
                        count_row["count_source"],
                        group_name,
                        None,
                    ),
                )
                n_counts += 1
        counts["raw_count"] = n_counts

        # ================================================================
        # 8. raw_vote_cast
        # ================================================================
        cast_rows = source_conn.execute(
            """
            SELECT rvc.cast_id, rvc.vote_event_id, rvc.legislator_name,
                   rvc.legislator_group, rvc.sentido
            FROM raw_vote_cast rvc
            JOIN raw_vote_event rve ON rve.vote_event_id = rvc.vote_event_id
            WHERE rve.chamber = ? AND rve.legislature = ?
            """,
            (chamber_source, legislature),
        ).fetchall()

        n_casts = 0
        for cast_id, src_ve_id, legislator_name, _group, sentido in cast_rows:
            target_ve_id = source_ve_to_target.get(src_ve_id)
            if target_ve_id is None:
                continue
            pkey = build_person_key(legislator_name)
            mkey = build_membership_key(pkey, chamber_source, legislature)
            membership_id = membership_key_to_id.get(mkey)
            if membership_id is None:
                continue
            vote_option = map_vote_option(sentido)
            conn.execute(
                "INSERT OR IGNORE INTO raw_vote_cast "
                "(raw_vote_event_id, raw_membership_id, vote_option, "
                "source_vote_cast_id, notes) "
                "VALUES (?, ?, ?, ?, ?)",
                (target_ve_id, membership_id, vote_option, str(cast_id), None),
            )
            n_casts += 1
        counts["raw_vote_cast"] = n_casts

        # ================================================================
        # FK verification
        # ================================================================
        fk_violations = conn.execute("PRAGMA foreign_key_check").fetchall()
        if fk_violations:
            raise RuntimeError(f"FK violations in raw.db: {fk_violations}")

        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()

    return counts
