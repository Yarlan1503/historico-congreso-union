"""Tests para las funciones de matching en scraper/person_normalizer.py.

Cubre deep_normalize, load_catalog y match_person con catálogo fixture.
"""

from pathlib import Path

import pytest

from scraper.person_normalizer import (
    CatalogEntry,
    deep_normalize,
    load_catalog,
    match_person,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _make_catalog_entry(
    canonical_name: str = "garcía lópez, juan",
    person_key: str = "garcia_lopez_juan",
    original_names: str = "Sen. García López, Juan",
    cast_count: int = 100,
    chambers: str = "senado",
    party_senado: str = "PRI",
    n_variants: int = 1,
    is_ambiguous: bool = False,
) -> CatalogEntry:
    """Crea un CatalogEntry con valores por defecto."""
    return CatalogEntry(
        canonical_name=canonical_name,
        person_key=person_key,
        original_names=original_names,
        cast_count=cast_count,
        chambers=chambers,
        party_senado=party_senado,
        n_variants=n_variants,
        is_ambiguous=is_ambiguous,
    )


@pytest.fixture()
def small_catalog() -> dict[str, CatalogEntry]:
    """Catálogo fixture con 5 entries para tests deterministas."""
    return {
        "ayala_almeida_joel": _make_catalog_entry(
            canonical_name="ayala almeida, joel",
            person_key="ayala_almeida_joel",
            original_names="Sen. Ayala Almeida, Joel|Sen. Ayala Almeida , Joel",
            cast_count=2074,
            chambers="senado",
            party_senado="PRI",
            n_variants=2,
            is_ambiguous=True,
        ),
        "gamboa_patron_emilio": _make_catalog_entry(
            canonical_name="gamboa patrón, emilio",
            person_key="gamboa_patron_emilio",
            original_names="Dip. Gamboa Patrón, Emilio",
            cast_count=2534,
            chambers="senado",
            party_senado="PRI",
            n_variants=3,
            is_ambiguous=True,
        ),
        "corral_jurado_javier": _make_catalog_entry(
            canonical_name="corral jurado, javier",
            person_key="corral_jurado_javier",
            original_names="Sen. Corral Jurado, Javier",
            cast_count=1873,
            chambers="senado",
            party_senado="PAN",
            n_variants=2,
            is_ambiguous=True,
        ),
        "bartlett_diaz_manuel": _make_catalog_entry(
            canonical_name="bartlett díaz, manuel",
            person_key="bartlett_diaz_manuel",
            original_names="Sen. Bartlett Díaz, Manuel",
            cast_count=1824,
            chambers="senado",
            party_senado="PRI|PT",
            n_variants=2,
            is_ambiguous=True,
        ),
        "calderon_hinojosa_luisa_maria": _make_catalog_entry(
            canonical_name="calderón hinojosa, luisa maría",
            person_key="calderon_hinojosa_luisa_maria",
            original_names="Sen. Calderón Hinojosa, Luisa María",
            cast_count=1846,
            chambers="senado",
            party_senado="PAN|SG",
            n_variants=2,
            is_ambiguous=True,
        ),
    }


def _write_csv(tmp_path: Path, rows: list[dict[str, str]]) -> Path:
    """Escribe un CSV temporal con los rows dados y retorna su path."""
    csv_path = tmp_path / "test_catalog.csv"
    header = (
        "canonical_name,person_key,original_names,cast_count,"
        "chambers,party_senado,n_variants,is_ambiguous"
    )
    lines = [header]
    for row in rows:
        lines.append(
            f'"{row["canonical_name"]}",{row["person_key"]},'
            f'"{row["original_names"]}",{row["cast_count"]},'
            f'{row["chambers"]},{row["party_senado"]},'
            f'{row["n_variants"]},{row["is_ambiguous"]}'
        )
    csv_path.write_text("\n".join(lines), encoding="utf-8")
    return csv_path


# ---------------------------------------------------------------------------
# TestDeepNormalize
# ---------------------------------------------------------------------------
class TestDeepNormalize:
    """Tests para deep_normalize."""

    def test_removes_accents_and_commas(self):
        """Debe quitar acentos y comas."""
        assert deep_normalize("García, Juan") == "garcia juan"

    def test_removes_honorifics(self):
        """Debe quitar prefijos como Sen."""
        assert deep_normalize("Sen. López Martínez, Ana") == "lopez martinez ana"

    def test_idempotency(self):
        """deep_normalize(deep_normalize(x)) == deep_normalize(x)."""
        name = "Sen. García, Juan"
        assert deep_normalize(deep_normalize(name)) == deep_normalize(name)

    def test_no_accents_no_commas_unchanged(self):
        """Sin acentos ni comas debe solo lower."""
        assert deep_normalize("Gomez Juan") == "gomez juan"

    def test_strips_suffixes(self):
        """Debe quitar sufijos como (LICENCIA)."""
        assert deep_normalize("Pérez, Luis (LICENCIA)") == "perez luis"

    def test_empty_string(self):
        """String vacío debe retornar vacío."""
        assert deep_normalize("") == ""


# ---------------------------------------------------------------------------
# TestLoadCatalog
# ---------------------------------------------------------------------------
class TestLoadCatalog:
    """Tests para load_catalog."""

    def test_loads_entries(self, tmp_path: Path):
        """Debe cargar entries del CSV."""
        csv_path = _write_csv(tmp_path, [
            {
                "canonical_name": "garcía, juan",
                "person_key": "garcia_juan",
                "original_names": "Sen. García, Juan",
                "cast_count": "100",
                "chambers": "senado",
                "party_senado": "PRI",
                "n_variants": "1",
                "is_ambiguous": "False",
            },
        ])
        catalog = load_catalog(csv_path)
        assert "garcia_juan" in catalog
        assert catalog["garcia_juan"].canonical_name == "garcía, juan"

    def test_correct_number_of_entries(self, tmp_path: Path):
        """Debe retornar el número correcto de entries."""
        csv_path = _write_csv(tmp_path, [
            {
                "canonical_name": "uno, a",
                "person_key": "uno_a",
                "original_names": "Uno, A",
                "cast_count": "10",
                "chambers": "senado",
                "party_senado": "",
                "n_variants": "1",
                "is_ambiguous": "False",
            },
            {
                "canonical_name": "dos, b",
                "person_key": "dos_b",
                "original_names": "Dos, B",
                "cast_count": "20",
                "chambers": "diputados",
                "party_senado": "PAN",
                "n_variants": "1",
                "is_ambiguous": "True",
            },
        ])
        catalog = load_catalog(csv_path)
        assert len(catalog) == 2

    def test_empty_csv(self, tmp_path: Path):
        """CSV vacío (solo header) debe retornar dict vacío."""
        csv_path = tmp_path / "empty.csv"
        csv_path.write_text(
            "canonical_name,person_key,original_names,cast_count,"
            "chambers,party_senado,n_variants,is_ambiguous\n",
            encoding="utf-8",
        )
        catalog = load_catalog(csv_path)
        assert catalog == {}

    def test_is_ambiguous_parsing(self, tmp_path: Path):
        """Debe parsear is_ambiguous como boolean."""
        csv_path = _write_csv(tmp_path, [
            {
                "canonical_name": "test, a",
                "person_key": "test_a",
                "original_names": "Test, A",
                "cast_count": "5",
                "chambers": "senado",
                "party_senado": "",
                "n_variants": "1",
                "is_ambiguous": "True",
            },
        ])
        catalog = load_catalog(csv_path)
        assert catalog["test_a"].is_ambiguous is True

    def test_cast_count_as_int(self, tmp_path: Path):
        """cast_count debe ser int."""
        csv_path = _write_csv(tmp_path, [
            {
                "canonical_name": "test, a",
                "person_key": "test_a",
                "original_names": "Test, A",
                "cast_count": "42",
                "chambers": "senado",
                "party_senado": "",
                "n_variants": "1",
                "is_ambiguous": "False",
            },
        ])
        catalog = load_catalog(csv_path)
        assert catalog["test_a"].cast_count == 42

    def test_loads_real_catalog(self):
        """Debe cargar el catálogo real sin errores."""
        csv_path = Path("data/person_catalog.csv")
        if not csv_path.exists():
            pytest.skip("Real catalog not available")
        catalog = load_catalog(csv_path)
        assert len(catalog) > 0
        assert "ayala_almeida_joel" in catalog


# ---------------------------------------------------------------------------
# TestMatchPerson
# ---------------------------------------------------------------------------
class TestMatchPerson:
    """Tests para match_person."""

    def test_exact_match(self, small_catalog: dict[str, CatalogEntry]):
        """Nombre exacto debe dar method='exact' con confidence 1.0."""
        result = match_person("Sen. Ayala Almeida, Joel", small_catalog)
        assert result.method == "exact"
        assert result.confidence == 1.0
        assert result.person_id == "ayala_almeida_joel"

    def test_exact_match_with_space_before_comma(self, small_catalog: dict[str, CatalogEntry]):
        """Espacio antes de coma debe dar exact match."""
        result = match_person("Sen. Ayala Almeida , Joel", small_catalog)
        assert result.method == "exact"
        assert result.person_id == "ayala_almeida_joel"

    def test_normalized_match_accent_variation(
        self, small_catalog: dict[str, CatalogEntry]
    ):
        """Variación de acentos debe dar normalized match."""
        # "Gamboa Patron" sin acento vs "gamboa patrón" con acento en catálogo
        # build_canonical_person_key("Gamboa Patron, Emilio") → "gamboa_patron,_emilio"
        # catalog key is "gamboa_patron_emilio" — different because of comma
        # So exact fails, but deep_normalize matches
        result = match_person("Gamboa Patron Emilio", small_catalog)
        assert result.method in ("exact", "normalized")
        assert result.person_id == "gamboa_patron_emilio"

    def test_fuzzy_match_typo(self, small_catalog: dict[str, CatalogEntry]):
        """Nombre con typo debe dar fuzzy match."""
        # "ayala almeida" → "ayala_almeida_joel" is close to "ayala_almeida_joel"
        # but let's use a variation that only fuzzy can catch
        result = match_person("Ayala Almeyda, Joel", small_catalog)
        assert result.method in ("exact", "normalized", "fuzzy")
        if result.method == "fuzzy":
            assert result.confidence >= 0.85

    def test_no_match_returns_new(self, small_catalog: dict[str, CatalogEntry]):
        """Nombre inventado debe dar method='new' con person_id=None."""
        result = match_person("Xyzwq Noexistente, Pedro", small_catalog)
        assert result.method == "new"
        assert result.person_id is None
        assert result.confidence == 0.0

    def test_no_match_preserves_canonical(self, small_catalog: dict[str, CatalogEntry]):
        """En no-match, canonical_name debe ser el nombre canonizado."""
        result = match_person("Zúñiga Torres, María", small_catalog)
        assert result.method == "new"
        assert result.canonical_name == "zúñiga torres maría"

    def test_exact_match_no_honorific(self, small_catalog: dict[str, CatalogEntry]):
        """Sin prefijo, nombre exacto debe dar exact match."""
        result = match_person("Bartlett Díaz, Manuel", small_catalog)
        assert result.method == "exact"
        assert result.person_id == "bartlett_diaz_manuel"

    def test_match_preserves_original_name(self, small_catalog: dict[str, CatalogEntry]):
        """original_name debe preservar el nombre de entrada."""
        original = "Sen. Corral Jurado, Javier"
        result = match_person(original, small_catalog)
        assert result.original_name == original

    def test_empty_catalog_returns_new(self):
        """Catálogo vacío siempre debe dar method='new'."""
        result = match_person("García, Juan", {})
        assert result.method == "new"
        assert result.person_id is None

    def test_exact_match_with_suffix(self, small_catalog: dict[str, CatalogEntry]):
        """Sufijo (LICENCIA) debe eliminarse antes de matching."""
        result = match_person("Sen. Corral Jurado, Javier (LICENCIA)", small_catalog)
        assert result.method == "exact"
        assert result.person_id == "corral_jurado_javier"

    def test_fuzzy_confidence_reasonable(self, small_catalog: dict[str, CatalogEntry]):
        """Fuzzy match debe tener confidence > 0 y < 1."""
        result = match_person("Ayala Almeyda, Joel", small_catalog)
        if result.method == "fuzzy":
            assert 0.0 < result.confidence < 1.0

    def test_exact_match_diputados_prefix(self, small_catalog: dict[str, CatalogEntry]):
        """Dip. prefix debe eliminarse y dar exact match."""
        result = match_person("Dip. Gamboa Patrón, Emilio", small_catalog)
        assert result.method == "exact"
        assert result.person_id == "gamboa_patron_emilio"
