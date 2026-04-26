"""Genera catálogo interno de legisladores desde datos scrapeados.

Lee todos los nombres únicos de raw_vote_cast, los normaliza con
canonical_name(), y genera data/person_catalog.csv con estadísticas.
"""
from __future__ import annotations

import csv
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

# Agregar raíz del proyecto al path para imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scraper.person_normalizer import canonical_name

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "historico.db"
OUTPUT_PATH = Path(__file__).resolve().parent.parent / "data" / "person_catalog.csv"


def _build_accent_map() -> dict:
    """Construir tabla de traducción para quitar acentos.

    Returns:
        Tabla de traducción usable con str.translate().
    """
    return str.maketrans("áéíóúÁÉÍÓÚñÑüÜ", "aeiouAEIOUnNuU")


ACCENT_MAP = _build_accent_map()


def _to_person_key(canonical: str) -> str:
    """Convertir canonical_name a person_key para matching.

    Elimina acentos, espacios y comas para generar una clave
    insensible a variaciones ortográficas menores.

    Args:
        canonical: Nombre canónico en minúsculas.

    Returns:
        Clave sin acentos con espacios reemplazados por underscores.
    """
    return canonical.replace(" ", "_").replace(",", "").translate(ACCENT_MAP)


def build_catalog(conn: sqlite3.Connection) -> tuple[list[dict], int]:
    """Construir el catálogo de personas desde la base de datos.

    Ejecuta query para obtener nombres únicos con metadatos, agrupa
    por canonical_name, detecta colisiones y genera lista de registros.

    Args:
        conn: Conexión a SQLite con row_factory configurado.

    Returns:
        Tupla (catálogo, total_nombres_originales) donde catálogo es
        una lista de dicts con las columnas del CSV.
    """
    query = """
        SELECT rvc.legislator_name,
               rve.chamber,
               COUNT(*) as cast_count,
               GROUP_CONCAT(DISTINCT rvc.legislator_group) as groups
        FROM raw_vote_cast rvc
        JOIN raw_vote_event rve ON rve.vote_event_id = rvc.vote_event_id
        GROUP BY rvc.legislator_name
        ORDER BY cast_count DESC
    """
    rows = conn.execute(query).fetchall()

    # Agrupar por canonical_name
    clusters: dict[str, dict] = defaultdict(
        lambda: {
            "original_names": set(),
            "cast_count": 0,
            "chambers": set(),
            "party_senado": None,
        }
    )

    total_original = 0
    for row in rows:
        total_original += 1
        raw_name = row["legislator_name"]
        chamber = row["chamber"]
        count = row["cast_count"]
        groups = row["groups"]

        canonical = canonical_name(raw_name)
        cluster = clusters[canonical]
        cluster["original_names"].add(raw_name)
        cluster["cast_count"] += count
        cluster["chambers"].add(chamber)

        # Solo senado tiene partido en legislator_group
        if chamber == "senado" and groups:
            # Si hay múltiples partidos, tomar el primero (o todos)
            party_list = [g.strip() for g in groups.split(",") if g.strip()]
            if party_list:
                # Registrar todos los partidos del senado
                if cluster["party_senado"] is None:
                    cluster["party_senado"] = set(party_list)
                else:
                    cluster["party_senado"].update(party_list)

    # Construir catálogo con detección de ambigüedad
    catalog = []
    for canonical, data in sorted(clusters.items(), key=lambda x: -x[1]["cast_count"]):
        original_names = sorted(data["original_names"])
        n_variants = len(original_names)
        is_ambiguous = n_variants > 1

        party_senado = "|".join(sorted(data["party_senado"])) if data["party_senado"] else ""
        chambers_str = "|".join(sorted(data["chambers"]))

        catalog.append({
            "canonical_name": canonical,
            "person_key": _to_person_key(canonical),
            "original_names": "|".join(original_names),
            "cast_count": data["cast_count"],
            "chambers": chambers_str,
            "party_senado": party_senado,
            "n_variants": n_variants,
            "is_ambiguous": is_ambiguous,
        })

    return catalog, total_original


def write_csv(catalog: list[dict], output_path: Path) -> None:
    """Escribir el catálogo a archivo CSV.

    Args:
        catalog: Lista de dicts con los datos del catálogo.
        output_path: Ruta de destino del archivo CSV.
    """
    fieldnames = [
        "canonical_name",
        "person_key",
        "original_names",
        "cast_count",
        "chambers",
        "party_senado",
        "n_variants",
        "is_ambiguous",
    ]
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(catalog)


def print_stats(catalog: list[dict], total_original: int) -> None:
    """Imprimir estadísticas del catálogo a stdout.

    Args:
        catalog: Lista de dicts con los datos del catálogo.
        total_original: Total de nombres originales únicos.
    """
    total_canonical = len(catalog)
    merged = total_original - total_canonical
    ambiguous = [r for r in catalog if r["is_ambiguous"]]

    # Por cámara
    diputados = sum(1 for r in catalog if "diputados" in r["chambers"])
    senado = sum(1 for r in catalog if "senado" in r["chambers"])
    both = sum(1 for r in catalog if "diputados" in r["chambers"] and "senado" in r["chambers"])
    dip_only = diputados - both
    sen_only = senado - both

    # Cobertura de partido
    with_party = sum(1 for r in catalog if r["party_senado"])
    without_party = total_canonical - with_party

    print("=== PERSON CATALOG STATISTICS ===")
    print(f"Total unique original names: {total_original}")
    print(f"Total canonical names: {total_canonical}")
    print(f"Names merged (duplicates fixed): {merged}")
    print(f"Ambiguous clusters: {len(ambiguous)}")
    print()
    print("By chamber:")
    print(f"  diputados: {dip_only} names")
    print(f"  senado: {sen_only} names")
    print(f"  both: {both} names")
    print()
    print("Party coverage:")
    print(f"  With party (senado): {with_party}")
    print(f"  Without party: {without_party}")
    print()

    # Top ambiguous clusters
    if ambiguous:
        top_ambiguous = sorted(ambiguous, key=lambda r: -r["n_variants"])[:5]
        print("Top ambiguous clusters:")
        for entry in top_ambiguous:
            names = entry["original_names"].split("|")
            print(f"  {entry['canonical_name']}")
            print(f"    variants ({entry['n_variants']}): {', '.join(names[:5])}")
            if len(names) > 5:
                print(f"    ... and {len(names) - 5} more")
            print(f"    chambers: {entry['chambers']}, party: {entry['party_senado'] or 'N/A'}")


def main() -> None:
    """Punto de entrada del script.

    Conecta a la base de datos, construye el catálogo de personas,
    lo escribe a CSV y muestra estadísticas.
    """
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        catalog, total_original = build_catalog(conn)
        write_csv(catalog, OUTPUT_PATH)
        print_stats(catalog, total_original)
        print(f"\nCSV written to {OUTPUT_PATH} ({len(catalog)} rows)")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
