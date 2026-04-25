"""Tests para scraper/exporter/ — mapping, artifacts, raw_db y snapshot.

Cubre:
- Funciones puras de mapping (normalize, keys, counts)
- Generación de artefactos JSON (manifest, quality_report)
- Flujo completo de export_snapshot con DB in-memory
"""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import pytest

from scraper.exporter.mapping import (
    build_membership_key,
    build_person_key,
    build_source_key,
    build_vote_event_key,
    counts_to_rows,
    map_vote_option,
    normalize_person_name,
    source_chamber_to_contract_camara,
)


# ============================================================================
# Fixtures
# ============================================================================

SCHEMA_PATH = Path(__file__).resolve().parent.parent / "f2" / "schema.sql"


@pytest.fixture
def source_db(tmp_path):
    """Crea una DB source en memoria con datos de prueba (2 VEs, 5 casts, 3 counts)."""
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON")

    # Crear schema completo
    conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))

    # 1. Source assets
    conn.execute(
        """
        INSERT INTO source_asset (source_tag, url, method, response_body_hash,
        status_code, content_type, captured_at, raw_body_path, run_id)
        VALUES ('dip_sitl', 'http://example.com/vote1', 'GET',
                'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                200, 'text/html', '2024-01-01T00:00:00+00:00',
                'data/raw/1', 'run_001')
        """
    )
    asset_id_1 = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    conn.execute(
        """
        INSERT INTO source_asset (source_tag, url, method, response_body_hash,
        status_code, content_type, captured_at, raw_body_path, run_id)
        VALUES ('dip_sitl', 'http://example.com/vote2', 'GET',
                'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
                200, 'text/html', '2024-01-02T00:00:00+00:00',
                'data/raw/2', 'run_001')
        """
    )
    asset_id_2 = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    # 2. Vote events (2)
    conn.execute(
        """
        INSERT INTO raw_vote_event (chamber, legislature, vote_date, title, subject, source_url)
        VALUES ('diputados', 'LXVI', '2024-01-15', 'Dictamen de Hacienda', 'Presupuesto 2024',
                'http://example.com/vote1')
        """
    )
    ve_id_1 = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    conn.execute(
        """
        INSERT INTO raw_vote_event (chamber, legislature, vote_date, title, subject, source_url)
        VALUES ('diputados', 'LXVI', '2024-01-16', 'Dictamen de Educación', 'Reforma educativa',
                'http://example.com/vote2')
        """
    )
    ve_id_2 = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    # 3. Vote event assets
    conn.execute(
        """
        INSERT INTO vote_event_asset (vote_event_id, asset_id, asset_role)
        VALUES (?, ?, 'primary_nominal')
        """,
        (ve_id_1, asset_id_1),
    )
    conn.execute(
        """
        INSERT INTO vote_event_asset (vote_event_id, asset_id, asset_role)
        VALUES (?, ?, 'primary_nominal')
        """,
        (ve_id_2, asset_id_2),
    )

    # 4. Vote casts (5)
    casts = [
        (ve_id_1, asset_id_1, "Juan Pérez", "MORENA", "a_favor"),
        (ve_id_1, asset_id_1, "Ana García", "PAN", "en_contra"),
        (ve_id_1, asset_id_1, "José López", "PRI", "abstencion"),
        (ve_id_2, asset_id_2, "Juan Pérez", "MORENA", "a_favor"),
        (ve_id_2, asset_id_2, "María Rodríguez", "MORENA", "ausente"),
    ]
    for ve_id, a_id, name, group, sentido in casts:
        conn.execute(
            """
            INSERT INTO raw_vote_cast (vote_event_id, asset_id, legislator_name,
                                       legislator_group, sentido)
            VALUES (?, ?, ?, ?, ?)
            """,
            (ve_id, a_id, name, group, sentido),
        )

    # 5. Vote counts (3 rows — 1 total + 1 group for VE1, 1 total for VE2)
    counts = [
        (ve_id_1, asset_id_1, None, 150, 80, 10, 5, 0, 0, 250),
        (ve_id_1, asset_id_1, "MORENA", 100, 10, 5, 2, 0, 0, 117),
        (ve_id_2, asset_id_2, None, 200, 50, 15, 10, 3, 0, 278),
    ]
    for ve_id, a_id, group, af, ec, ab, au, nv, pr, total in counts:
        conn.execute(
            """
            INSERT INTO vote_counts (vote_event_id, asset_id, group_name,
            a_favor, en_contra, abstencion, ausente, novoto, presente, total)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (ve_id, a_id, group, af, ec, ab, au, nv, pr, total),
        )

    conn.commit()
    yield conn
    conn.close()


@pytest.fixture
def source_db_file(source_db, tmp_path):
    """Escribe la DB in-memory a un archivo para usar con export_snapshot."""
    db_path = tmp_path / "test_source.db"
    file_conn = sqlite3.connect(str(db_path))
    source_db.backup(file_conn)
    file_conn.close()
    return db_path


# ============================================================================
# 1. Tests unitarios de mapping.py
# ============================================================================


class TestNormalizePersonName:
    def test_basic_normalization(self):
        assert normalize_person_name("  JUAN PÉREZ  ") == "Juan Pérez"

    def test_multiple_spaces(self):
        assert normalize_person_name("María   García   López") == "María García López"

    def test_already_normalized(self):
        assert normalize_person_name("Ana López") == "Ana López"

    def test_empty_after_strip(self):
        # strip → "", split → [], join → "", title → ""
        assert normalize_person_name("   ") == ""


class TestBuildPersonKey:
    def test_deterministic(self):
        key1 = build_person_key("Juan Pérez")
        key2 = build_person_key("Juan Pérez")
        assert key1 == key2

    def test_normalizes(self):
        assert build_person_key("Juan Pérez") == "juan_perez"

    def test_accent_stripping(self):
        assert build_person_key("José García") == "jose_garcia"

    def test_uppercase_input(self):
        assert build_person_key("JUAN PÉREZ") == "juan_perez"


class TestCountsToRows:
    def test_filters_zeros(self):
        result = counts_to_rows(
            {
                "a_favor": 10,
                "en_contra": 5,
                "abstencion": 0,
                "ausente": 2,
                "novoto": 0,
                "presente": 1,
            }
        )
        assert len(result) == 4
        options = [r["option"] for r in result]
        assert "a_favor" in options
        assert "en_contra" in options
        assert "ausente" in options
        assert "presente" in options

    def test_novoto_maps_to_no_vote(self):
        result = counts_to_rows(
            {
                "a_favor": 0,
                "en_contra": 0,
                "abstencion": 0,
                "ausente": 0,
                "novoto": 3,
                "presente": 0,
            }
        )
        assert len(result) == 1
        assert result[0]["option"] == "no_vote"

    def test_count_source_always_published_raw(self):
        result = counts_to_rows(
            {
                "a_favor": 1,
                "en_contra": 0,
                "abstencion": 0,
                "ausente": 0,
                "novoto": 0,
                "presente": 0,
            }
        )
        assert result[0]["count_source"] == "published_raw"


class TestMapVoteOption:
    def test_novoto_to_no_vote(self):
        assert map_vote_option("novoto") == "no_vote"

    def test_a_favor_unchanged(self):
        assert map_vote_option("a_favor") == "a_favor"

    def test_invalid_raises(self):
        with pytest.raises(ValueError):
            map_vote_option("invalid")


class TestSourceChamberToContract:
    def test_diputados(self):
        assert source_chamber_to_contract_camara("diputados") == "D"

    def test_senado(self):
        assert source_chamber_to_contract_camara("senado") == "S"


class TestKeyBuilders:
    def test_vote_event_key_format(self):
        key = build_vote_event_key("diputados", "LXVI", "http://example.com")
        assert key == "diputados::LXVI::http://example.com"

    def test_membership_key_format(self):
        key = build_membership_key("juan_perez", "diputados", "LXVI")
        assert key == "juan_perez::diputados::LXVI"

    def test_source_key_format(self):
        key = build_source_key("sitl", "http://x.com", "abc123")
        assert key == "sitl::http://x.com::abc123"


# ============================================================================
# 2. Test de flujo completo con DB in-memory
# ============================================================================


class TestExportSnapshotFull:
    """Test el flujo completo de export_snapshot."""

    def test_creates_all_artifacts(self, source_db_file, tmp_path):
        """Verifica que se crean todos los artefactos esperados."""
        from scraper.exporter.snapshot import export_snapshot

        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        output_base = tmp_path / "snapshots"

        result = export_snapshot(
            db_path=source_db_file,
            raw_dir=raw_dir,
            output_base=output_base,
            chamber_source="diputados",
            legislature="LXVI",
        )

        snapshot_dir = Path(result["snapshot_dir"])

        assert (snapshot_dir / "manifest.json").exists()
        assert (snapshot_dir / "cache_index.json").exists()
        assert (snapshot_dir / "quality_report.json").exists()
        assert (snapshot_dir / "provenance.json").exists()
        assert (snapshot_dir / "raw.db").exists()

    def test_raw_db_has_correct_schema(self, source_db_file, tmp_path):
        """Verifica que raw.db tiene el schema correcto con todas las tablas."""
        from scraper.exporter.snapshot import export_snapshot

        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        output_base = tmp_path / "snapshots"

        result = export_snapshot(
            db_path=source_db_file,
            raw_dir=raw_dir,
            output_base=output_base,
            chamber_source="diputados",
            legislature="LXVI",
        )

        snapshot_dir = Path(result["snapshot_dir"])
        raw_db = sqlite3.connect(str(snapshot_dir / "raw.db"))
        raw_db.execute("PRAGMA foreign_keys = ON")

        tables = {
            row[0]
            for row in raw_db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            ).fetchall()
        }

        expected_tables = {
            "raw_import_batch",
            "raw_source",
            "raw_provenance",
            "raw_person",
            "raw_post",
            "raw_membership",
            "raw_motion",
            "raw_vote_event",
            "raw_count",
            "raw_vote_cast",
            "raw_partial_diagnostic",
        }
        assert expected_tables.issubset(tables), f"Missing tables: {expected_tables - tables}"

        violations = raw_db.execute("PRAGMA foreign_key_check").fetchall()
        assert violations == [], f"FK violations: {violations}"

        raw_db.close()

    def test_raw_db_row_counts(self, source_db_file, tmp_path):
        """Verifica conteos de rows en raw.db."""
        from scraper.exporter.snapshot import export_snapshot

        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        output_base = tmp_path / "snapshots"

        result = export_snapshot(
            db_path=source_db_file,
            raw_dir=raw_dir,
            output_base=output_base,
            chamber_source="diputados",
            legislature="LXVI",
        )

        snapshot_dir = Path(result["snapshot_dir"])
        raw_db = sqlite3.connect(str(snapshot_dir / "raw.db"))

        # 4 personas únicas (Juan Pérez, Ana García, José López, María Rodríguez)
        n_persons = raw_db.execute("SELECT COUNT(*) FROM raw_person").fetchone()[0]
        assert n_persons == 4

        # 4 memberships (1 por persona)
        n_memberships = raw_db.execute("SELECT COUNT(*) FROM raw_membership").fetchone()[0]
        assert n_memberships == 4

        # 2 motions + 2 vote events
        n_motions = raw_db.execute("SELECT COUNT(*) FROM raw_motion").fetchone()[0]
        assert n_motions == 2
        n_ves = raw_db.execute("SELECT COUNT(*) FROM raw_vote_event").fetchone()[0]
        assert n_ves == 2

        # 5 vote casts
        n_casts = raw_db.execute("SELECT COUNT(*) FROM raw_vote_cast").fetchone()[0]
        assert n_casts == 5

        # Counts: verificar que hay al menos algunos counts (>0)
        n_counts = raw_db.execute("SELECT COUNT(*) FROM raw_count").fetchone()[0]
        assert n_counts > 0

        raw_db.close()

    def test_quality_report_has_warn_status(self, source_db_file, tmp_path):
        """Verifica que quality_report.json tiene status "warn"."""
        from scraper.exporter.snapshot import export_snapshot

        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        output_base = tmp_path / "snapshots"

        result = export_snapshot(
            db_path=source_db_file,
            raw_dir=raw_dir,
            output_base=output_base,
            chamber_source="diputados",
            legislature="LXVI",
        )

        snapshot_dir = Path(result["snapshot_dir"])
        qr = json.loads((snapshot_dir / "quality_report.json").read_text())

        assert qr["status"] == "warn"
        assert qr["contract_version"] == "0.1"
        assert len(qr["checks"]) >= 1
        assert any(c["status"] == "warn" for c in qr["checks"])

    def test_manifest_valid(self, source_db_file, tmp_path):
        """Verifica que manifest.json tiene campos requeridos."""
        from scraper.exporter.snapshot import export_snapshot

        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        output_base = tmp_path / "snapshots"

        result = export_snapshot(
            db_path=source_db_file,
            raw_dir=raw_dir,
            output_base=output_base,
            chamber_source="diputados",
            legislature="LXVI",
        )

        snapshot_dir = Path(result["snapshot_dir"])
        manifest = json.loads((snapshot_dir / "manifest.json").read_text())

        assert manifest["contract_version"] == "0.1"
        assert manifest["camara"] == "D"
        assert manifest["legislatura"] == "LXVI"
        assert "cache_index" in manifest["artifacts"]
        assert "quality_report" in manifest["artifacts"]
        assert "provenance" in manifest["artifacts"]
        assert manifest["package_id"].startswith("snapshot_D_LXVI_")

    def test_idempotency(self, source_db_file, tmp_path):
        """Verifica que segundo export con mismo package_id falla (idempotencia)."""
        from scraper.exporter.snapshot import export_snapshot

        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        output_base = tmp_path / "snapshots"

        frozen_now = datetime(2026, 4, 25, 12, 0, 0, tzinfo=UTC)

        with patch("scraper.exporter.snapshot.datetime") as mock_dt:
            mock_dt.now.return_value = frozen_now

            # Primer export — OK
            result1 = export_snapshot(
                db_path=source_db_file,
                raw_dir=raw_dir,
                output_base=output_base,
                chamber_source="diputados",
                legislature="LXVI",
            )
            assert "snapshot_dir" in result1

            # Segundo export con mismo timestamp → mismo package_id → FileExistsError
            with pytest.raises(FileExistsError):
                export_snapshot(
                    db_path=source_db_file,
                    raw_dir=raw_dir,
                    output_base=output_base,
                    chamber_source="diputados",
                    legislature="LXVI",
                )

    def test_rejects_senado(self, tmp_path):
        """Verifica que Senado es rechazado."""
        from scraper.exporter.snapshot import export_snapshot

        with pytest.raises(ValueError, match="diputados"):
            export_snapshot(
                db_path=tmp_path / "dummy.db",
                raw_dir=tmp_path / "raw",
                output_base=tmp_path / "snapshots",
                chamber_source="senado",
                legislature="LXVI",
            )

    def test_rejects_invalid_legislature(self, source_db_file, tmp_path):
        """Verifica que legislaturas fuera de scope son rechazadas."""
        from scraper.exporter.snapshot import export_snapshot

        with pytest.raises(ValueError, match="scope"):
            export_snapshot(
                db_path=source_db_file,
                raw_dir=tmp_path / "raw",
                output_base=tmp_path / "snapshots",
                chamber_source="diputados",
                legislature="LXII",
            )


# ============================================================================
# 3. Tests de artifacts unitarios
# ============================================================================


class TestBuildQualityReport:
    def test_default_warn(self):
        from scraper.exporter.artifacts import build_quality_report

        qr = build_quality_report("D", "LXVI")
        assert qr["status"] == "warn"
        assert qr["contract_version"] == "0.1"

    def test_with_disambiguation_pass(self):
        from scraper.exporter.artifacts import build_quality_report

        qr = build_quality_report("D", "LXVI", person_disambiguation=True)
        # person_disambiguation=pass, counts_consistency=skip → overall=pass
        assert qr["status"] == "pass"


class TestBuildManifest:
    def test_required_fields(self):
        from scraper.exporter.artifacts import build_manifest

        manifest = build_manifest(
            package_id="test_pkg",
            source_package_id="run_001",
            camara="D",
            legislatura="LXVI",
            artifact_files={
                "cache_index": "ci.json",
                "quality_report": "qr.json",
                "provenance": "p.json",
            },
        )
        assert manifest["contract_version"] == "0.1"
        assert manifest["camara"] == "D"
        assert manifest["legislatura"] == "LXVI"
        assert "notes" not in manifest

    def test_with_notes(self):
        from scraper.exporter.artifacts import build_manifest

        manifest = build_manifest(
            package_id="test_pkg",
            source_package_id="run_001",
            camara="D",
            legislatura="LXVI",
            artifact_files={
                "cache_index": "ci.json",
                "quality_report": "qr.json",
                "provenance": "p.json",
            },
            notes="Test note",
        )
        assert manifest["notes"] == "Test note"
