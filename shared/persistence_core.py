"""Funciones puras de inserción idempotente en SQLite.

No realizan commit; el caller controla la transacción.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import date, datetime
from pathlib import Path

logger = logging.getLogger(__name__)


def _normalize_str(value) -> str | None:
    """Normaliza enums, Path, HttpUrl, etc. a str."""
    if value is None:
        return None
    if hasattr(value, "value"):
        return str(value.value)
    return str(value)


def _normalize_datetime(value) -> str | None:
    """Normaliza datetime/date a ISO str."""
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _normalize_json(value) -> str | None:
    """Normaliza dict a JSON str; deja str/None intactos."""
    if value is None:
        return None
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False)
    return value


def insert_source_asset(conn: sqlite3.Connection, asset_dict: dict) -> tuple[int, bool]:
    """Inserta (o ignora) un ``SourceAsset`` y devuelve ``(asset_id, was_inserted)``.

    ``asset_dict`` puede provenir de un dict plano o de un modelo Pydantic
    serializado (e.g. via ``model_dump(mode='json')``).
    """
    cursor = conn.execute(
        """
        INSERT OR IGNORE INTO source_asset
        (source_tag, url, method, request_payload_hash, response_body_hash,
         response_headers_hash, status_code, content_type, encoding,
         captured_at, waf_detected, cache_detected, repetition_num,
         run_id, raw_body_path)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            _normalize_str(asset_dict["source_tag"]),
            _normalize_str(asset_dict["url"]),
            _normalize_str(asset_dict.get("method")),
            asset_dict.get("request_payload_hash"),
            asset_dict["response_body_hash"],
            asset_dict.get("response_headers_hash"),
            asset_dict.get("status_code"),
            asset_dict.get("content_type"),
            asset_dict.get("encoding"),
            _normalize_datetime(asset_dict["captured_at"]),
            int(asset_dict.get("waf_detected", False)),
            int(asset_dict.get("cache_detected", False)),
            asset_dict.get("repetition_num", 1),
            asset_dict.get("run_id"),
            _normalize_str(asset_dict.get("raw_body_path")),
        ),
    )
    was_inserted = cursor.rowcount > 0

    row = conn.execute(
        "SELECT asset_id FROM source_asset WHERE source_tag=? AND url=? AND response_body_hash=?",
        (
            _normalize_str(asset_dict["source_tag"]),
            _normalize_str(asset_dict["url"]),
            asset_dict["response_body_hash"],
        ),
    ).fetchone()
    if not row:
        raise RuntimeError("No se pudo recuperar asset_id después de INSERT OR IGNORE")
    return row[0], was_inserted


def insert_raw_vote_event(conn: sqlite3.Connection, event_dict: dict) -> tuple[int, bool]:
    """Inserta o actualiza un ``RawVoteEvent`` vía UPSERT.

    Devuelve ``(vote_event_id, was_inserted)``.
    """
    source_url = _normalize_str(event_dict.get("source_url"))
    chamber = _normalize_str(event_dict["chamber"])
    legislature = _normalize_str(event_dict["legislature"])

    # SELECT previo solo para determinar was_inserted; el UPSERT siempre ejecuta
    existing = conn.execute(
        """
        SELECT vote_event_id FROM raw_vote_event
        WHERE chamber=? AND legislature=? AND source_url=?
        """,
        (chamber, legislature, source_url),
    ).fetchone()
    was_inserted = existing is None

    cursor = conn.execute(
        """
        INSERT INTO raw_vote_event
        (chamber, legislature, vote_date, title, subject, source_url, metadata_json)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(chamber, legislature, source_url)
        DO UPDATE SET
            vote_date = excluded.vote_date,
            title = excluded.title,
            subject = excluded.subject,
            metadata_json = excluded.metadata_json
        RETURNING vote_event_id
        """,
        (
            chamber,
            legislature,
            _normalize_datetime(event_dict.get("vote_date")),
            event_dict.get("title"),
            event_dict.get("subject"),
            source_url,
            _normalize_json(event_dict.get("metadata_json")),
        ),
    )
    row = cursor.fetchone()
    if row is None:
        raise RuntimeError("UPSERT de raw_vote_event no devolvió vote_event_id")
    return row[0], was_inserted


def insert_vote_event_asset(conn: sqlite3.Connection, link_dict: dict) -> bool:
    """Inserta (o ignora) la relación vote_event ↔ asset. Devuelve ``was_inserted``."""
    cursor = conn.execute(
        """
        INSERT OR IGNORE INTO vote_event_asset
        (vote_event_id, asset_id, asset_role)
        VALUES (?, ?, ?)
        """,
        (
            link_dict["vote_event_id"],
            link_dict["asset_id"],
            _normalize_str(link_dict["asset_role"]),
        ),
    )
    return cursor.rowcount > 0


def insert_raw_vote_casts(conn: sqlite3.Connection, casts: list[dict]) -> int:
    """Inserta votos nominales con ``INSERT OR IGNORE``. Devuelve cantidad insertada."""
    inserted = 0
    for cast in casts:
        cursor = conn.execute(
            """
            INSERT OR IGNORE INTO raw_vote_cast
            (vote_event_id, asset_id, legislator_name, legislator_group,
             sentido, region, raw_row_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                cast["vote_event_id"],
                cast["asset_id"],
                cast["legislator_name"],
                cast.get("legislator_group"),
                _normalize_str(cast["sentido"]),
                cast.get("region"),
                _normalize_json(cast.get("raw_row_json")),
            ),
        )
        if cursor.rowcount > 0:
            inserted += 1
    return inserted


def insert_vote_counts(conn: sqlite3.Connection, counts_list: list[dict]) -> int:
    """Inserta o actualiza conteos vía UPSERT (group_name NOT NULL) o deduplicación manual (group_name IS NULL).

    Devuelve cantidad de filas afectadas (insertadas o actualizadas).
    """
    affected = 0
    for vc in counts_list:
        group_name = vc.get("group_name")
        if group_name is None:
            # SQLite UNIQUE trata NULLs como distintos → deduplicación manual
            existing = conn.execute(
                """
                SELECT 1 FROM vote_counts
                WHERE vote_event_id=? AND asset_id=? AND group_name IS NULL
                """,
                (vc["vote_event_id"], vc["asset_id"]),
            ).fetchone()
            if existing:
                continue
            cursor = conn.execute(
                """
                INSERT INTO vote_counts
                (vote_event_id, asset_id, group_name, a_favor, en_contra,
                 abstencion, ausente, novoto, presente, total)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    vc["vote_event_id"],
                    vc["asset_id"],
                    group_name,
                    vc.get("a_favor", 0),
                    vc.get("en_contra", 0),
                    vc.get("abstencion", 0),
                    vc.get("ausente", 0),
                    vc.get("novoto", 0),
                    vc.get("presente", 0),
                    vc.get("total"),
                ),
            )
            affected += cursor.rowcount
        else:
            cursor = conn.execute(
                """
                INSERT INTO vote_counts
                (vote_event_id, asset_id, group_name, a_favor, en_contra,
                 abstencion, ausente, novoto, presente, total)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(vote_event_id, asset_id, group_name)
                DO UPDATE SET
                    a_favor = excluded.a_favor,
                    en_contra = excluded.en_contra,
                    abstencion = excluded.abstencion,
                    ausente = excluded.ausente,
                    novoto = excluded.novoto,
                    presente = excluded.presente,
                    total = excluded.total
                """,
                (
                    vc["vote_event_id"],
                    vc["asset_id"],
                    group_name,
                    vc.get("a_favor", 0),
                    vc.get("en_contra", 0),
                    vc.get("abstencion", 0),
                    vc.get("ausente", 0),
                    vc.get("novoto", 0),
                    vc.get("presente", 0),
                    vc.get("total"),
                ),
            )
            affected += cursor.rowcount
    return affected
