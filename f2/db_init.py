#!/usr/bin/env python3
"""Inicializa la base de datos SQLite productiva para historico-congreso-union.

Uso:
    python f2/db_init.py
    python f2/db_init.py --db-path /tmp/test_historico.db
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path


DEFAULT_DB_RELATIVE = Path("data") / "historico.db"


def resolve_project_root() -> Path:
    """Resuelve la raíz del proyecto asumiendo que este script vive en f2/."""
    return Path(__file__).resolve().parent.parent


def init_db(db_path: Path) -> None:
    """Crea (o actualiza idempotentemente) el schema en ``db_path``."""
    # Asegurar que los directorios intermedios existen
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA busy_timeout = 5000;")

    # Leer schema.sql desde el mismo directorio que este script
    schema_file = Path(__file__).with_name("schema.sql")
    if not schema_file.exists():
        raise FileNotFoundError(f"No se encontró el archivo de schema: {schema_file}")

    schema_sql = schema_file.read_text(encoding="utf-8")

    # Ejecutar el script completo.  SQLite maneja IF NOT EXISTS de forma
    # idempotente, por lo que re-ejecutar no genera errores.
    conn.executescript(schema_sql)
    conn.commit()
    conn.close()


def verify_db(db_path: Path) -> bool:
    """Verifica básica: PRAGMAs activos y lista de tablas."""
    conn = sqlite3.connect(str(db_path))

    # Activar PRAGMAs de conexión antes de verificar (igual que haría
    # cualquier consumidor de la base de datos).
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA busy_timeout = 5000;")

    # Verificar PRAGMAs
    journal_mode = conn.execute("PRAGMA journal_mode;").fetchone()[0]
    foreign_keys = conn.execute("PRAGMA foreign_keys;").fetchone()[0]
    busy_timeout = conn.execute("PRAGMA busy_timeout;").fetchone()[0]

    ok = True
    if journal_mode.lower() != "wal":
        print(f"  ❌ journal_mode = {journal_mode} (esperado: wal)", file=sys.stderr)
        ok = False
    else:
        print(f"  ✅ journal_mode = {journal_mode}")

    if foreign_keys != 1:
        print(f"  ❌ foreign_keys = {foreign_keys} (esperado: 1)", file=sys.stderr)
        ok = False
    else:
        print(f"  ✅ foreign_keys = {foreign_keys}")

    if busy_timeout != 5000:
        print(f"  ❌ busy_timeout = {busy_timeout} (esperado: 5000)", file=sys.stderr)
        ok = False
    else:
        print(f"  ✅ busy_timeout = {busy_timeout}")

    # Verificar tablas creadas
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"
    )
    tables = [row[0] for row in cursor.fetchall()]
    expected_tables = {
        "source_asset",
        "raw_vote_event",
        "vote_event_asset",
        "raw_vote_cast",
        "vote_counts",
    }
    found = set(tables) & expected_tables
    missing = expected_tables - found
    if missing:
        print(f"  ❌ Tablas faltantes: {missing}", file=sys.stderr)
        ok = False
    else:
        print(f"  ✅ Tablas esperadas presentes: {sorted(found)}")

    conn.close()
    return ok


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Inicializa la base de datos SQLite del proyecto historico-congreso-union."
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=resolve_project_root() / DEFAULT_DB_RELATIVE,
        help=(
            "Ruta al archivo SQLite a crear/inicializar "
            f"(default: {DEFAULT_DB_RELATIVE} relativo a la raíz del proyecto)"
        ),
    )
    args = parser.parse_args(argv)

    db_path: Path = args.db_path.resolve()
    print(f"📦 Inicializando base de datos: {db_path}")
    init_db(db_path)
    print("📝 Verificando schema y PRAGMAs...")
    if verify_db(db_path):
        print("✅ Verificación completada exitosamente.")
        return 0
    else:
        print("❌ Verificación falló.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
