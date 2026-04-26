"""Tests para scraper/person_normalizer.py."""

from scraper.person_normalizer import (
    PersonMatch,
    build_canonical_person_key,
    canonical_name,
    fix_mojibake,
    normalize_whitespace,
    strip_honorifics,
    strip_suffixes,
)


def _mojibake(s: str) -> str:
    """Simula mojibake: codifica en UTF-8 y decodifica como Latin-1."""
    return s.encode("utf-8").decode("latin-1")


# ---------------------------------------------------------------------------
# 1. STRIP HONORIFICS
# ---------------------------------------------------------------------------
class TestStripHonorifics:
    """Tests para strip_honorifics."""

    def test_sen_prefix(self):
        """Sen. debe ser removido."""
        assert strip_honorifics("Sen. García, Juan") == "García, Juan"

    def test_dip_prefix(self):
        """Dip. debe ser removido."""
        assert strip_honorifics("Dip. Chanona Burguete, Alejandro") == (
            "Chanona Burguete, Alejandro"
        )

    def test_senadora_prefix(self):
        """Senadora debe ser removido."""
        assert strip_honorifics("Senadora López") == "López"

    def test_senador_prefix(self):
        """Senador debe ser removido."""
        assert strip_honorifics("Senador Gamboa Patrón, Emilio") == "Gamboa Patrón, Emilio"

    def test_diputada_prefix(self):
        """Diputada debe ser removido."""
        assert strip_honorifics("Diputada Ávila, María") == "Ávila, María"

    def test_diputado_prefix(self):
        """Diputado debe ser removido."""
        assert strip_honorifics("Diputado Ruiz, Carlos") == "Ruiz, Carlos"

    def test_no_prefix_unchanged(self):
        """Sin prefijo debe devolver sin cambios."""
        assert strip_honorifics("García, Juan") == "García, Juan"

    def test_case_insensitive(self):
        """Debe funcionar con minúsculas."""
        assert strip_honorifics("sen. García, Juan") == "García, Juan"


# ---------------------------------------------------------------------------
# 2. FIX MOJIBAKE
# ---------------------------------------------------------------------------
class TestFixMojibake:
    """Tests para fix_mojibake."""

    def test_o_acute(self):
        """Mojibake de ó (Ã³) debe corregirse."""
        assert fix_mojibake(_mojibake("Gómez")) == "Gómez"

    def test_a_acute(self):
        """Mojibake de á (Ã¡) debe corregirse."""
        assert fix_mojibake(_mojibake("Sánchez")) == "Sánchez"

    def test_no_mojibake_unchanged(self):
        """String correcto debe devolverse sin cambios."""
        assert fix_mojibake("Gómez") == "Gómez"

    def test_i_acute(self):
        """Mojibake de í (Ã­) debe corregirse."""
        assert fix_mojibake(_mojibake("Martínez")) == "Martínez"

    def test_empty_string(self):
        """String vacío debe devolver vacío."""
        assert fix_mojibake("") == ""

    def test_multiple_accents(self):
        """Múltiples acentos en un solo string."""
        assert fix_mojibake(_mojibake("Sánchez Cordero Dávila")) == "Sánchez Cordero Dávila"

    def test_literal_mojibake_gomez(self):
        """Test con literal mojibake conocido de la DB."""
        assert fix_mojibake("GÃ³mez Urrutia NapoleÃ³n") == "Gómez Urrutia Napoleón"

    def test_literal_mojibake_sanchez(self):
        """Test con literal mojibake Sánchez Cordero Dávila."""
        assert fix_mojibake("SÃ¡nchez Cordero DÃ¡vila") == "Sánchez Cordero Dávila"


# ---------------------------------------------------------------------------
# 3. NORMALIZE WHITESPACE
# ---------------------------------------------------------------------------
class TestNormalizeWhitespace:
    """Tests para normalize_whitespace."""

    def test_space_before_comma(self):
        """Espacio antes de coma debe eliminarse."""
        assert normalize_whitespace("Ayala Almeida , Joel") == "Ayala Almeida, Joel"

    def test_multiple_spaces(self):
        """Múltiples espacios deben colapsarse."""
        assert normalize_whitespace("García  López  Juan") == "García López Juan"

    def test_leading_trailing_spaces(self):
        """Espacios al inicio y final deben eliminarse."""
        assert normalize_whitespace("  García  ") == "García"

    def test_normal_string_unchanged(self):
        """String normal debe quedar sin cambios."""
        assert normalize_whitespace("García López") == "García López"


# ---------------------------------------------------------------------------
# 4. STRIP SUFFIXES
# ---------------------------------------------------------------------------
class TestStripSuffixes:
    """Tests para strip_suffixes."""

    def test_licencia(self):
        """(LICENCIA) debe ser removido."""
        result = strip_suffixes("Castro Trenti Fernando Jorge (LICENCIA)")
        assert result == "Castro Trenti Fernando Jorge"

    def test_sup(self):
        """(SUP) debe ser removido."""
        assert strip_suffixes("García López Juan (SUP)") == "García López Juan"

    def test_propietario(self):
        """(PROPIETARIO) debe ser removido."""
        assert strip_suffixes("Ruiz Martínez Ana (PROPIETARIO)") == "Ruiz Martínez Ana"

    def test_lic_abbreviation(self):
        """(LIC) debe ser removido."""
        assert strip_suffixes("Pérez García Luis (LIC)") == "Pérez García Luis"

    def test_suplente(self):
        """(SUPLENTE) debe ser removido."""
        assert strip_suffixes("López Díaz María (SUPLENTE)") == "López Díaz María"

    def test_prop_abbreviation(self):
        """(PROP) debe ser removido."""
        assert strip_suffixes("Hernández Torres José (PROP)") == "Hernández Torres José"

    def test_no_suffix_unchanged(self):
        """Sin sufijo debe devolver sin cambios."""
        assert strip_suffixes("García López Juan") == "García López Juan"


# ---------------------------------------------------------------------------
# 5. CANONICAL NAME
# ---------------------------------------------------------------------------
class TestCanonicalName:
    """Tests para canonical_name."""

    def test_senado_with_space_before_comma(self):
        """Pipeline completo: Senado con prefijo y espacio antes de coma."""
        assert canonical_name("Sen. Ayala Almeida , Joel") == "ayala almeida joel"

    def test_licencia_suffix(self):
        """Pipeline con LICENCIA."""
        result = canonical_name("Castro Trenti Fernando Jorge (LICENCIA)")
        assert result == "castro trenti fernando jorge"

    def test_mojibake_input(self):
        """Pipeline con mojibake."""
        assert canonical_name(_mojibake("Gómez Urrutia Napoleón")) == "gómez urrutia napoleón"

    def test_diputados_no_prefix(self):
        """Pipeline con diputados sin prefijo."""
        assert canonical_name("Abreu Artiñano Rocío Adriana") == "abreu artiñano rocío adriana"

    def test_idempotency(self):
        """canonical_name(canonical_name(x)) == canonical_name(x)."""
        name = "Sen. Ayala Almeida , Joel"
        assert canonical_name(canonical_name(name)) == canonical_name(name)

    def test_senado_space_comma_pairs(self):
        """Las 7 parejas space-coma de Senado deben normalizarse al mismo valor."""
        pairs = [
            ("Sen. Ayala Almeida , Joel", "Sen. Ayala Almeida, Joel"),
            ("Sen. Bartlett Díaz , Manuel", "Sen. Bartlett Díaz, Manuel"),
            (
                "Sen. Calderón Hinojosa , Luisa María",
                "Sen. Calderón Hinojosa, Luisa María",
            ),
            ("Sen. Corral Jurado , Javier", "Sen. Corral Jurado, Javier"),
            ("Sen. Gamboa Patrón , Emilio", "Sen. Gamboa Patrón, Emilio"),
            (
                "Sen. González Martínez , Jorge Emilio",
                "Sen. González Martínez, Jorge Emilio",
            ),
            ("Sen. Larios Córdova , Héctor", "Sen. Larios Córdova, Héctor"),
        ]
        for spacey, normal in pairs:
            assert canonical_name(spacey) == canonical_name(normal), (
                f"Mismatch: {spacey!r} vs {normal!r}"
            )

    def test_dip_with_prefix(self):
        """Diputados con prefijo Dip."""
        assert canonical_name("Dip. Cerda Pérez, Rogelio") == "cerda pérez rogelio"

    def test_pure_name_no_modifications(self):
        """Nombre puro sin prefijos ni sufijos."""
        assert canonical_name("Acosta Ruiz José Carlos") == "acosta ruiz josé carlos"

    def test_idempotency_mojibake(self):
        """Idempotencia con mojibake en entrada."""
        name = _mojibake("Sánchez Cordero Dávila")
        assert canonical_name(canonical_name(name)) == canonical_name(name)


# ---------------------------------------------------------------------------
# 6. BUILD CANONICAL PERSON KEY
# ---------------------------------------------------------------------------
class TestBuildCanonicalPersonKey:
    """Tests para build_canonical_person_key."""

    def test_senador_with_comma(self):
        """Senador con coma debe generar key con coma."""
        assert build_canonical_person_key("Sen. García, Juan") == "garcia_juan"

    def test_deterministic(self):
        """Misma entrada debe producir misma salida."""
        name = "Sen. Gamboa Patrón, Emilio"
        assert build_canonical_person_key(name) == build_canonical_person_key(name)

    def test_removes_accents(self):
        """Debe quitar acentos de la key."""
        assert "ó" not in build_canonical_person_key("Gómez Urrutia Napoleón")
        assert "á" not in build_canonical_person_key("Sánchez Cordero")

    def test_licencia_removed(self):
        """LICENCIA no debe aparecer en la key."""
        key = build_canonical_person_key("Castro Trenti Fernando Jorge (LICENCIA)")
        assert "licencia" not in key
        assert key == "castro_trenti_fernando_jorge"

    def test_space_before_comma_key_match(self):
        """Espacio antes de coma no debe afectar la key."""
        key1 = build_canonical_person_key("Sen. Ayala Almeida , Joel")
        key2 = build_canonical_person_key("Sen. Ayala Almeida, Joel")
        assert key1 == key2

    def test_n_preserved_as_n(self):
        """ñ debe convertirse a n en la key."""
        key = build_canonical_person_key("Muñoz García")
        assert "ñ" not in key
        assert key == "munoz_garcia"


# ---------------------------------------------------------------------------
# 7. PERSONMATCH DATACLASS
# ---------------------------------------------------------------------------
class TestPersonMatch:
    """Tests para PersonMatch dataclass."""

    def test_instantiation(self):
        """PersonMatch debe instanciarse correctamente."""
        pm = PersonMatch(
            method="exact",
            confidence=1.0,
            canonical_name="garcía, juan",
            person_id="abc123",
            original_name="Sen. García, Juan",
        )
        assert pm.method == "exact"
        assert pm.confidence == 1.0
        assert pm.canonical_name == "garcía, juan"
        assert pm.person_id == "abc123"
        assert pm.original_name == "Sen. García, Juan"

    def test_default_values(self):
        """Valores por defecto de person_id y original_name."""
        pm = PersonMatch(method="new", confidence=0.0, canonical_name="test")
        assert pm.person_id is None
        assert pm.original_name == ""
