"""Tests para persistencia SQLite."""

import sqlite3

import pytest


@pytest.fixture
def db_conn():
    """Conexión SQLite en memoria con schema mínimo inicializado."""
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 5000")

    # Schema mínimo copiado de f2/schema.sql (solo source_asset)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS source_asset (
            asset_id               INTEGER PRIMARY KEY AUTOINCREMENT,
            source_tag             TEXT NOT NULL,  -- Sin CHECK: acepta cualquier string
            url                    TEXT NOT NULL,
            method                 TEXT NOT NULL DEFAULT 'GET'
                                       CHECK(method IN ('GET', 'POST')),
            request_payload_hash   TEXT,
            response_body_hash     TEXT NOT NULL,
            response_headers_hash  TEXT,
            status_code            INTEGER,
            content_type           TEXT,
            encoding               TEXT,
            captured_at            TEXT,
            waf_detected           INTEGER NOT NULL DEFAULT 0
                                       CHECK(waf_detected IN (0, 1)),
            cache_detected         INTEGER NOT NULL DEFAULT 0
                                       CHECK(cache_detected IN (0, 1)),
            repetition_num         INTEGER NOT NULL DEFAULT 1,
            run_id                 TEXT,
            raw_body_path          TEXT NOT NULL,
            UNIQUE(source_tag, url, response_body_hash)
        )
        """
    )
    conn.commit()
    yield conn
    conn.close()


def test_insert_and_read_source_asset(db_conn):
    """Inserta un registro en source_asset y verifica que se puede leer."""
    cursor = db_conn.execute(
        """
        INSERT INTO source_asset
        (source_tag, url, method, response_body_hash, status_code,
         captured_at, raw_body_path)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "dip_sitl",
            "https://example.com/voto",
            "GET",
            "deadbeef",
            200,
            "2024-01-01T00:00:00+00:00",
            "xraw/test",
        ),
    )
    db_conn.commit()
    asset_id = cursor.lastrowid

    row = db_conn.execute(
        "SELECT asset_id, source_tag, url, response_body_hash FROM source_asset WHERE asset_id = ?",
        (asset_id,),
    ).fetchone()

    assert row is not None
    assert row[0] == asset_id
    assert row[1] == "dip_sitl"
    assert row[2] == "https://example.com/voto"
    assert row[3] == "deadbeef"
