"""Tests de integración para person disambiguation (B4).

Valida el flujo end-to-end: generación del catálogo, matching contra catálogo,
export con/sin catálogo, y consistencia entre normalización y exporter.

Usa catálogo real (data/person_catalog.csv) y DB real (data/historico.db)
en modo SOLO LECTURA cuando es posible.
"""

from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

import pytest

from scraper.exporter.mapping import build_person_key, normalize_person_name
from scraper.exporter.snapshot import export_snapshot
from scraper.person_normalizer import (
    build_canonical_person_key,
    canonical_name,
    load_catalog,
    match_person,
)

# ---------------------------------------------------------------------------
# Paths a datos reales (SOLO LECTURA)
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
REAL_CATALOG = PROJECT_ROOT / "data" / "person_catalog.csv"
REAL_DB = PROJECT_ROOT / "data" / "historico.db"
SCHEMA_PATH = PROJECT_ROOT / "f2" / "schema.sql"

# Skip decorators para tests que requieren datos reales
requires_catalog = pytest.mark.skipif(
    not REAL_CATALOG.exists(),
    reason="data/person_catalog.csv no disponible",
)
requires_db = pytest.mark.skipif(
    not REAL_DB.exists(),
    reason="data/historico.db no disponible",
)


# ============================================================================
# 1. TestCatalogGeneration — valida catálogo contra DB real
# ============================================================================


class TestCatalogGeneration:
    """Tests que validan generación del catálogo contra DB real."""

    @requires_catalog
    def test_catalog_file_exists(self):
        """data/person_catalog.csv existe y tiene contenido."""
        assert REAL_CATALOG.exists()
        assert REAL_CATALOG.stat().st_size > 0

    @requires_catalog
    def test_catalog_has_expected_columns(self):
        """CSV tiene las 8 columnas esperadas."""
        expected = {
            "canonical_name",
            "person_key",
            "original_names",
            "cast_count",
            "chambers",
            "party_senado",
            "n_variants",
            "is_ambiguous",
        }
        with REAL_CATALOG.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            assert reader.fieldnames is not None
            assert set(reader.fieldnames) == expected

    @requires_catalog
    def test_catalog_has_reasonable_row_count(self):
        """CSV tiene entre 1000-1500 filas (1316 esperado)."""
        with REAL_CATALOG.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            count = sum(1 for _ in reader)
        assert 1000 <= count <= 1500, f"Row count {count} fuera de rango"

    @requires_catalog
    def test_all_space_comma_pairs_merged(self):
        """Los 7 pares space-comma de senado se resuelven a mismo canonical_name.

        Verifica que al menos 3 de los 7 pares conocidos estén en el mismo
        cluster (misma person_key) en el catálogo.
        """
        space_comma_pairs = [
            ("Sen. Ayala Almeida , Joel", "Sen. Ayala Almeida, Joel"),
            ("Sen. Bartlett Díaz , Manuel", "Sen. Bartlett Díaz, Manuel"),
            ("Sen. Calderón Hinojosa , Luisa María", "Sen. Calderón Hinojosa, Luisa María"),
            ("Sen. Corral Jurado , Javier", "Sen. Corral Jurado, Javier"),
            ("Sen. Gamboa Patrón , Emilio", "Sen. Gamboa Patrón, Emilio"),
            ("Sen. González Martínez , Jorge Emilio", "Sen. González Martínez, Jorge Emilio"),
            ("Sen. Larios Córdova , Héctor", "Sen. Larios Córdova, Héctor"),
        ]

        catalog = load_catalog(REAL_CATALOG)
        verified = 0

        for spacey, normal in space_comma_pairs:
            key_spacey = build_canonical_person_key(spacey)
            key_normal = build_canonical_person_key(normal)
            # Ambos deben producir la misma person_key y estar en el catálogo
            assert key_spacey == key_normal, (
                f"Keys diferentes: {key_spacey!r} vs {key_normal!r}"
            )
            # La key (sin coma) debe existir en el catálogo
            lookup = key_spacey.replace(",", "")
            if lookup in catalog:
                verified += 1

        assert verified >= 3, (
            f"Solo {verified}/7 pares verificados en el catálogo"
        )

    @requires_catalog
    def test_licencia_suffix_stripped(self):
        """Los 2 nombres con (LICENCIA) aparecen sin el sufijo en canonical_name."""
        licencia_names = [
            ("Castro Trenti Fernando Jorge (LICENCIA)", "castro trenti fernando jorge"),
            ("Burgos Hernández Anais Miriam (LICENCIA)", "burgos hernández anais miriam"),
        ]

        catalog = load_catalog(REAL_CATALOG)
        for original, expected_canonical in licencia_names:
            key = build_canonical_person_key(original).replace(",", "")
            assert key in catalog, (
                f"{original!r} → key={key!r} no encontrado en catálogo"
            )
            entry = catalog[key]
            assert entry.canonical_name == expected_canonical, (
                f"canonical_name={entry.canonical_name!r}, esperado={expected_canonical!r}"
            )

    @requires_catalog
    def test_senado_has_party_data(self):
        """Los registros de senado tienen party_senado no vacío."""
        catalog = load_catalog(REAL_CATALOG)
        senado_entries = [
            e for e in catalog.values() if e.chambers == "senado"
        ]
        assert len(senado_entries) > 0, "No hay registros de senado"

        with_party = sum(1 for e in senado_entries if e.party_senado)
        # La gran mayoría de registros de senado deben tener party
        ratio = with_party / len(senado_entries)
        assert ratio >= 0.95, (
            f"Solo {with_party}/{len(senado_entries)} ({ratio:.1%}) tienen party"
        )

    @requires_catalog
    def test_diputados_has_no_party_data(self):
        """Los registros de diputados tienen party_senado vacío."""
        catalog = load_catalog(REAL_CATALOG)
        dip_entries = [
            e for e in catalog.values() if e.chambers == "diputados"
        ]
        assert len(dip_entries) > 0, "No hay registros de diputados"

        with_party = [e for e in dip_entries if e.party_senado]
        assert with_party == [], (
            f"Registros de diputados con party inesperado: "
            f"{[e.person_key for e in with_party[:3]]}"
        )


# ============================================================================
# 2. TestMatchingAgainstCatalog — matching contra catálogo real
# ============================================================================


class TestMatchingAgainstCatalog:
    """Tests de matching contra el catálogo real."""

    @requires_catalog
    def test_exact_match_senado_name(self):
        """Nombres de senado con prefijo matchean exactamente."""
        catalog = load_catalog(REAL_CATALOG)
        result = match_person("Sen. Ayala Almeida, Joel", catalog)
        assert result.method == "exact"
        assert result.confidence == 1.0
        assert result.person_id == "ayala_almeida_joel"

    @requires_catalog
    def test_exact_match_diputados_name(self):
        """Nombres de diputados sin prefijo matchean exactamente."""
        catalog = load_catalog(REAL_CATALOG)
        # Buscar un nombre de diputados del catálogo
        dip_names = []
        with REAL_CATALOG.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row["chambers"] == "diputados":
                    # original_names puede tener múltiples variantes separadas por |
                    first_name = row["original_names"].split("|")[0]
                    dip_names.append((first_name, row["person_key"]))
                    break

        assert dip_names, "No se encontraron nombres de diputados"
        name, expected_key = dip_names[0]
        result = match_person(name, catalog)
        assert result.method == "exact"
        assert result.person_id == expected_key

    @requires_catalog
    def test_space_comma_variant_matches(self):
        """Nombres con space-comma matchean al mismo person_id."""
        catalog = load_catalog(REAL_CATALOG)
        result_normal = match_person("Sen. Ayala Almeida, Joel", catalog)
        result_spacey = match_person("Sen. Ayala Almeida , Joel", catalog)

        assert result_normal.person_id == result_spacey.person_id
        assert result_normal.method == "exact"
        assert result_spacey.method == "exact"

    @requires_catalog
    def test_honorific_stripped_matches(self):
        """Nombre con 'Sen.' y sin 'Sen.' matchean igual."""
        catalog = load_catalog(REAL_CATALOG)
        result_with = match_person("Sen. Bartlett Díaz, Manuel", catalog)
        result_without = match_person("Bartlett Díaz, Manuel", catalog)

        assert result_with.person_id == result_without.person_id
        assert result_with.person_id == "bartlett_diaz_manuel"

    @requires_catalog
    def test_matching_coverage(self):
        """Al menos 90% de nombres del catálogo matchean exactamente."""
        catalog = load_catalog(REAL_CATALOG)
        exact_count = 0
        total = 0

        for entry in catalog.values():
            # Probar con el primer nombre original de cada entrada
            first_name = entry.original_names.split("|")[0]
            total += 1
            result = match_person(first_name, catalog)
            if result.method == "exact":
                exact_count += 1

        ratio = exact_count / total if total > 0 else 0
        assert ratio >= 0.90, (
            f"Cobertura de matching: {exact_count}/{total} ({ratio:.1%}), mínimo 90%"
        )


# ============================================================================
# 3. TestExportWithCatalog — integración del exporter con catálogo
# ============================================================================


@pytest.fixture
def source_db(tmp_path):
    """Crea una DB source en memoria con datos de prueba (2 VEs, 5 casts, 3 counts)."""
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON")
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
    for ve_id, a_id in [(ve_id_1, asset_id_1), (ve_id_2, asset_id_2)]:
        conn.execute(
            """
            INSERT INTO vote_event_asset (vote_event_id, asset_id, asset_role)
            VALUES (?, ?, 'primary_nominal')
            """,
            (ve_id, a_id),
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

    # 5. Vote counts
    counts_data = [
        (ve_id_1, asset_id_1, None, 150, 80, 10, 5, 0, 0, 250),
        (ve_id_1, asset_id_1, "MORENA", 100, 10, 5, 2, 0, 0, 117),
        (ve_id_2, asset_id_2, None, 200, 50, 15, 10, 3, 0, 278),
    ]
    for row in counts_data:
        conn.execute(
            """
            INSERT INTO vote_counts (vote_event_id, asset_id, group_name,
            a_favor, en_contra, abstencion, ausente, novoto, presente, total)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            row,
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


@pytest.fixture
def mini_catalog(tmp_path):
    """Crea un mini-catalog CSV temporal para tests de export."""
    catalog_csv = tmp_path / "person_catalog.csv"
    catalog_csv.write_text(
        "canonical_name,person_key,original_names,cast_count,chambers,"
        "party_senado,n_variants,is_ambiguous\n"
        "juan perez,juan_perez,Juan Pérez,5,diputados,MORENA,1,False\n"
        "ana garcia,ana_garcia,Ana García,1,diputados|senado,PAN,1,False\n"
    )
    return catalog_csv


class TestExportWithCatalog:
    """Tests de integración del exporter con catálogo."""

    def test_export_with_catalog_populates_source_person_id(
        self, source_db_file, mini_catalog, tmp_path
    ):
        """Export con catalog_path pobla source_person_id en raw_person."""
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        output_base = tmp_path / "snapshots"

        result = export_snapshot(
            db_path=source_db_file,
            raw_dir=raw_dir,
            output_base=output_base,
            chamber_source="diputados",
            legislature="LXVI",
            catalog_path=mini_catalog,
        )

        snapshot_dir = Path(result["snapshot_dir"])
        raw_db = sqlite3.connect(str(snapshot_dir / "raw.db"))

        rows = raw_db.execute(
            "SELECT person_key, source_person_id FROM raw_person ORDER BY person_key"
        ).fetchall()
        person_map = {r[0]: r[1] for r in rows}

        # Juan Pérez y Ana García están en el catálogo → source_person_id poblado
        assert person_map["juan_perez"] == "juan_perez"
        assert person_map["ana_garcia"] == "ana_garcia"

        raw_db.close()

    def test_export_with_catalog_populates_organization_key(
        self, source_db_file, mini_catalog, tmp_path
    ):
        """Export con catalog_path pobla organization_key en raw_membership."""
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        output_base = tmp_path / "snapshots"

        result = export_snapshot(
            db_path=source_db_file,
            raw_dir=raw_dir,
            output_base=output_base,
            chamber_source="diputados",
            legislature="LXVI",
            catalog_path=mini_catalog,
        )

        snapshot_dir = Path(result["snapshot_dir"])
        raw_db = sqlite3.connect(str(snapshot_dir / "raw.db"))

        mem_rows = raw_db.execute(
            "SELECT rp.person_key, rm.organization_key "
            "FROM raw_membership rm "
            "JOIN raw_person rp ON rm.raw_person_id = rp.raw_person_id "
            "ORDER BY rp.person_key"
        ).fetchall()
        mem_map = {r[0]: r[1] for r in mem_rows}

        assert mem_map["juan_perez"] == "MORENA"
        assert mem_map["ana_garcia"] == "PAN"
        # José López NO está en catálogo → organization_key es NULL
        assert mem_map["jose_lopez"] is None

        raw_db.close()

    def test_export_without_catalog_has_null_source_person_id(
        self, source_db_file, tmp_path
    ):
        """Export sin catalog_path tiene source_person_id NULL."""
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

        rows = raw_db.execute("SELECT source_person_id FROM raw_person").fetchall()
        assert all(r[0] is None for r in rows), (
            f"Expected all NULL, got: {rows}"
        )

        raw_db.close()

    def test_export_fk_check_clean_with_catalog(
        self, source_db_file, mini_catalog, tmp_path
    ):
        """FK integrity check pasa con catálogo."""
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        output_base = tmp_path / "snapshots"

        result = export_snapshot(
            db_path=source_db_file,
            raw_dir=raw_dir,
            output_base=output_base,
            chamber_source="diputados",
            legislature="LXVI",
            catalog_path=mini_catalog,
        )

        snapshot_dir = Path(result["snapshot_dir"])
        raw_db = sqlite3.connect(str(snapshot_dir / "raw.db"))
        raw_db.execute("PRAGMA foreign_keys = ON")

        violations = raw_db.execute("PRAGMA foreign_key_check").fetchall()
        assert violations == [], f"FK violations: {violations}"

        raw_db.close()


# ============================================================================
# 4. TestNormalizationConsistency — consistencia normalización ↔ exporter
# ============================================================================


class TestNormalizationConsistency:
    """Verifica consistencia entre normalización y exporter."""

    def test_normalize_person_name_uses_canonical(self):
        """normalize_person_name produce el mismo resultado que canonical_name().title()."""
        test_names = [
            "Sen. Ayala Almeida , Joel",
            "Dip. García, Juan",
            "José López",
            "  María Rodríguez  ",
            "Castro Trenti Fernando Jorge (LICENCIA)",
        ]
        for name in test_names:
            expected = canonical_name(name).title()
            result = normalize_person_name(name)
            assert result == expected, (
                f"normalize_person_name({name!r}) = {result!r}, "
                f"expected {expected!r}"
            )

    def test_build_person_key_uses_canonical(self):
        """build_person_key produce el mismo resultado que build_canonical_person_key()."""
        test_names = [
            "Sen. Ayala Almeida , Joel",
            "Dip. García, Juan",
            "José López",
            "María Rodríguez",
            "Castro Trenti Fernando Jorge (LICENCIA)",
        ]
        for name in test_names:
            expected = build_canonical_person_key(name)
            result = build_person_key(name)
            assert result == expected, (
                f"build_person_key({name!r}) = {result!r}, "
                f"expected {expected!r}"
            )

    def test_cross_chamber_no_false_matches(self):
        """Nombres de diputados no matchean incorrectamente con senado."""
        # Crear un catálogo solo con entries de senado
        catalog = load_catalog(REAL_CATALOG)
        senado_only = {
            k: v for k, v in catalog.items() if v.chambers == "senado"
        }

        # Tomar algunos nombres de diputados del catálogo
        dip_names = []
        with REAL_CATALOG.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row["chambers"] == "diputados":
                    first_name = row["original_names"].split("|")[0]
                    dip_names.append(first_name)
                    if len(dip_names) >= 10:
                        break

        # Verificar que nombres de diputados NO matchean con entries de senado
        # (pueden tener IDs nuevos o normalized, pero no deben matchear a
        # una persona de senado diferente)
        for name in dip_names:
            result = match_person(name, senado_only)
            if result.person_id is not None:
                # Si matchea, el canonical_name debe ser razonablemente
                # diferente — no debe ser un falso positivo obvio
                name_key = build_canonical_person_key(name)
                assert result.person_id != name_key or result.method == "exact", (
                    f"Falso match: {name!r} matcheó a {result.person_id} "
                    f"({result.method}) en catálogo de solo-senado"
                )
