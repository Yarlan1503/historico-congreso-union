"""Mapeos source → target para el exporter popolo-congreso-union v0.1.

Funciones puras de transformación entre el schema del source DB (f2/schema.sql)
y el schema target (raw_v0_1.sql de popolo-congreso-union). No acceden a DB ni
filesystem; solo transforman datos en memoria.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Tabla de transliteración para quitar acentos (sin dependencias externas)
# ---------------------------------------------------------------------------
_ACCENT_MAP = str.maketrans(
    "áéíóúÁÉÍÓÚñÑüÜ",
    "aeiouAEIOUnNuU",
)

# ---------------------------------------------------------------------------
# Mapeo de opciones de voto: sentido (source) → option (target)
# ---------------------------------------------------------------------------
COUNT_OPTION_MAP: dict[str, str] = {
    "a_favor": "a_favor",
    "en_contra": "en_contra",
    "abstencion": "abstencion",
    "ausente": "ausente",
    "novoto": "no_vote",
    "presente": "presente",
}

# Mapeo inverso: option (target) → sentido (source)
OPTION_TO_SENTIDO: dict[str, str] = {v: k for k, v in COUNT_OPTION_MAP.items()}


# ---------------------------------------------------------------------------
# 1. CHAMBER MAPPING
# ---------------------------------------------------------------------------
def source_chamber_to_contract_camara(chamber: str) -> str:
    """Convierte chamber del source al formato camara del contrato JSON.

    El source usa ``'diputados'``/``'senado'``; los artefactos JSON usan
    ``'D'``/``'S'`` según el enum del schema de popolo-congreso-union.

    Args:
        chamber: Valor de chamber en el source (``'diputados'`` o ``'senado'``).

    Returns:
        Código de cámara del contrato (``'D'`` o ``'S'``).

    Raises:
        ValueError: Si *chamber* no es un valor reconocido.
    """
    mapping: dict[str, str] = {
        "diputados": "D",
        "senado": "S",
    }
    if chamber not in mapping:
        raise ValueError(
            f"Chamber no reconocido: {chamber!r}. "
            f"Valores válidos: {sorted(mapping)}"
        )
    return mapping[chamber]


# ---------------------------------------------------------------------------
# 2. PERSON KEY / NAME
# ---------------------------------------------------------------------------
def normalize_person_name(name: str) -> str:
    """Normaliza nombre de legislador para usar como base de person_key.

    - Elimina espacios al inicio y final.
    - Aplica Title Case (``str.title()``).
    - Colapsa múltiples espacios consecutivos en uno solo.

    Args:
        name: Nombre crudo del legislador.

    Returns:
        Nombre normalizado.

    Examples:
        >>> normalize_person_name("  JUAN PÉREZ  ")
        'Juan Pérez'
    """
    # strip primero, luego colapsar espacios internos, luego title
    stripped = name.strip()
    collapsed = " ".join(stripped.split())
    return collapsed.title()


def build_person_key(name: str) -> str:
    """Construye person_key determinista desde un nombre de legislador.

    Proceso:
    1. Normalizar el nombre con :func:`normalize_person_name`.
    2. Convertir a minúsculas.
    3. Reemplazar espacios con ``'_'``.
    4. Quitar acentos y diacríticos comunes (á→a, é→e, ñ→n, etc.).

    Es determinista: la misma entrada siempre produce la misma salida.

    Args:
        name: Nombre del legislador (crudo o normalizado).

    Returns:
        Clave determinista para ``raw_person.person_key``.

    Examples:
        >>> build_person_key("Juan Pérez")
        'juan_perez'
    """
    normalized = normalize_person_name(name)
    lower = normalized.lower()
    no_spaces = lower.replace(" ", "_")
    return no_spaces.translate(_ACCENT_MAP)


# ---------------------------------------------------------------------------
# 3. MEMBERSHIP KEY
# ---------------------------------------------------------------------------
def build_membership_key(person_key: str, chamber: str, legislature: str) -> str:
    """Construye membership_key determinista.

    Formato: ``"{person_key}::{chamber}::{legislature}"`` donde *chamber*
    es el valor del source (``'diputados'``), NO el código del contrato.

    Args:
        person_key: Clave de persona ya construida.
        chamber: Cámara en formato source (``'diputados'`` o ``'senado'``).
        legislature: Clave de legislatura (e.g. ``'LXVI'``).

    Returns:
        Clave determinista para ``raw_membership.membership_key``.

    Examples:
        >>> build_membership_key("juan_perez", "diputados", "LXVI")
        'juan_perez::diputados::LXVI'
    """
    return f"{person_key}::{chamber}::{legislature}"


# ---------------------------------------------------------------------------
# 4. MOTION KEY
# ---------------------------------------------------------------------------
def build_motion_key(chamber: str, legislature: str, source_url: str) -> str:
    """Construye motion_key determinista.

    Misma lógica que :func:`build_vote_event_key`. El campo *chamber* usa el
    valor del source (``'diputados'``).

    Args:
        chamber: Cámara en formato source.
        legislature: Clave de legislatura.
        source_url: URL original de la fuente.

    Returns:
        Clave determinista para ``raw_motion.motion_key``.

    Examples:
        >>> build_motion_key("diputados", "LXVI", "http://example.com/v/1")
        'diputados::LXVI::http://example.com/v/1'
    """
    return f"{chamber}::{legislature}::{source_url}"


# ---------------------------------------------------------------------------
# 5. VOTE EVENT KEY
# ---------------------------------------------------------------------------
def build_vote_event_key(chamber: str, legislature: str, source_url: str) -> str:
    """Construye vote_event_key determinista.

    Formato: ``"{chamber}::{legislature}::{source_url}"`` donde *chamber*
    es el valor del source (``'diputados'``).

    Args:
        chamber: Cámara en formato source.
        legislature: Clave de legislatura.
        source_url: URL original de la fuente.

    Returns:
        Clave determinista para ``raw_vote_event.vote_event_key``.

    Examples:
        >>> build_vote_event_key("diputados", "LXVI", "http://example.com")
        'diputados::LXVI::http://example.com'
    """
    return f"{chamber}::{legislature}::{source_url}"


# ---------------------------------------------------------------------------
# 6. SOURCE KEY
# ---------------------------------------------------------------------------
def build_source_key(source_tag: str, url: str, hash_sha256: str) -> str:
    """Construye source_key para raw_source.

    Formato: ``"{source_tag}::{url}::{hash_sha256}"``.

    Args:
        source_tag: Etiqueta del source (e.g. ``'sitl_lxvi'``).
        url: URL del asset original.
        hash_sha256: Hash SHA-256 del cuerpo de la respuesta.

    Returns:
        Clave determinista para ``raw_source.source_key``.

    Examples:
        >>> build_source_key("sitl", "http://x.com", "abc123")
        'sitl::http://x.com::abc123'
    """
    return f"{source_tag}::{url}::{hash_sha256}"


# ---------------------------------------------------------------------------
# 7. COUNTS NORMALIZATION
# ---------------------------------------------------------------------------
def counts_to_rows(count_dict: dict[str, int]) -> list[dict[str, str | int]]:
    """Normaliza las 6 columnas de vote_counts a rows para raw_count.

    Recibe un diccionario con las 6 opciones de voto del source y produce
    una lista de diccionarios listos para insertar en ``raw_count``, incluyendo
    solo las opciones con valor > 0.

    Mapeo de opciones especiales: ``novoto`` → ``'no_vote'``.

    Args:
        count_dict: Diccionario con claves ``a_favor``, ``en_contra``,
            ``abstencion``, ``ausente``, ``novoto``, ``presente`` y sus
            valores enteros.

    Returns:
        Lista de dicts con claves ``option``, ``count_value`` y
        ``count_source`` (siempre ``'published_raw'``).

    Examples:
        >>> rows = counts_to_rows({"a_favor": 10, "en_contra": 5,
        ...     "abstencion": 0, "ausente": 2, "novoto": 0, "presente": 1})
        >>> len(rows)
        4
        >>> rows[0]["option"]
        'a_favor'
    """
    rows: list[dict[str, str | int]] = []
    for source_key, target_option in COUNT_OPTION_MAP.items():
        value = count_dict.get(source_key, 0)
        if value > 0:
            rows.append({
                "option": target_option,
                "count_value": value,
                "count_source": "published_raw",
            })
    return rows


# ---------------------------------------------------------------------------
# 8. VOTE OPTION MAPPING
# ---------------------------------------------------------------------------
def map_vote_option(sentido: str) -> str:
    """Mapea sentido del source a vote_option del target.

    Mapeo especial: ``novoto`` → ``'no_vote'``; el resto se mantiene igual.

    Args:
        sentido: Valor de la columna ``sentido`` en ``raw_vote_cast``.
            Valores válidos: ``a_favor``, ``en_contra``, ``abstencion``,
            ``ausente``, ``novoto``, ``presente``.

    Returns:
        Valor de ``vote_option`` para ``raw_vote_cast``.

    Raises:
        ValueError: Si *sentido* no es un valor reconocido.

    Examples:
        >>> map_vote_option("novoto")
        'no_vote'
        >>> map_vote_option("a_favor")
        'a_favor'
    """
    if sentido not in COUNT_OPTION_MAP:
        raise ValueError(
            f"Sentido no reconocido: {sentido!r}. "
            f"Valores válidos: {sorted(COUNT_OPTION_MAP)}"
        )
    return COUNT_OPTION_MAP[sentido]
