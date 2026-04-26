#!/usr/bin/env python3
"""Backfill vote_date en raw_vote_event desde páginas índice SITL.

Lee config.toml para obtener legislaturas/periodos/URLs.
Fetch cada página índice, extrae {votacion_id: fecha_str} y actualiza
la DB solo donde vote_date IS NULL.

NOTA: Usa un parser propio (_extract_dates_from_index) que ignora
filas "mega" (100+ celdas) para máxima robustez. _extract_votacion_ids
del scraper ya fue corregido para manejar las filas mega correctamente.

Protecciones:
  - Solo UPDATE donde vote_date IS NULL (no sobreescribe)
  - Delay de 1s entre requests HTTP (evitar WAF)
  - Transacción por legislatura (commit al final de cada una)
  - Report de progreso en stderr
"""

import re
import sys
import time
import sqlite3
import tomllib
from pathlib import Path
from urllib.parse import urlparse, parse_qs

import httpx
from bs4 import BeautifulSoup

# Agregar raíz del proyecto al path para importar scraper
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scraper.sources.sitl import _parse_spanish_date, _DATE_RE

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "historico.db"
CONFIG_PATH = Path(__file__).resolve().parent.parent / "scraper" / "config.toml"

DELAY_SECONDS = 1.0  # Entre requests para evitar WAF


def load_config() -> dict:
    with open(CONFIG_PATH, "rb") as f:
        return tomllib.load(f)


def _extract_dates_from_index(body: bytes) -> dict[str, str | None]:
    """Extrae {votacion_id: fecha_str} del HTML índice SITL.

    A diferencia de _extract_votacion_ids del scraper, este parser:
    1. Ignora las filas "mega" (>3 celdas) que contienen links de múltiples fechas
    2. Solo extrae IDs de filas individuales (≤3 celdas) donde la fecha vigente es correcta
    3. Permite sobreescribir IDs con fechas (el primer match puede ser None)
    """
    try:
        text = body.decode("utf-8", errors="replace")
    except Exception:
        text = body.decode("iso-8859-1", errors="replace")

    soup = BeautifulSoup(text, "html.parser")
    current_date: str | None = None
    result: dict[str, str | None] = {}

    for row in soup.find_all("tr"):
        cells = row.find_all(["td", "th"])
        row_text = row.get_text(" ", strip=True)

        # Date rows: ≤3 cells and matches Spanish date pattern
        if len(cells) <= 3:
            m = _DATE_RE.search(row_text)
            if m:
                current_date = row_text
                continue

        # Only process individual vote rows (≤3 cells with a votaciont link)
        if len(cells) > 3:
            # Skip mega rows — they contain links from multiple dates
            continue

        for anchor in row.find_all("a", href=True):
            href = anchor["href"]
            if "votaciont=" not in href:
                continue
            parsed = urlparse(href)
            params = parse_qs(parsed.query)
            for val in params.get("votaciont", []):
                if val:
                    # Always update — later correct date overwrites earlier None
                    result[val] = current_date

    return dict(sorted(result.items()))


def fetch_index_dates(config: dict, legislature: str) -> dict[str, str | None]:
    """Fetch todas las páginas índice de una legislatura y retorna {votacion_id: fecha_str}."""
    leg = config["legislatures"].get(legislature, {})
    urls = leg.get("urls", {}).get("diputados", {})
    periods = leg.get("periods", {}).get("diputados_sitl", [])
    index_tpl = urls.get("sitl_index", "")

    if not index_tpl or not periods:
        return {}

    all_dates: dict[str, str | None] = {}
    client = httpx.Client(timeout=15.0, follow_redirects=True)

    for period in periods:
        url = index_tpl.format(periodo=period)
        print(f"  Fetching period {period}...", file=sys.stderr)
        try:
            resp = client.get(url)
            resp.raise_for_status()
            dates = _extract_dates_from_index(resp.content)
            all_dates.update(dates)
            dates_with_value = sum(1 for v in dates.values() if v is not None)
            print(f"    → {len(dates)} votaciones ({dates_with_value} with dates)", file=sys.stderr)
        except Exception as e:
            print(f"    ERROR: {e}", file=sys.stderr)
        time.sleep(DELAY_SECONDS)

    client.close()
    return all_dates


def extract_votacion_id(source_url: str) -> str | None:
    """Extrae votaciont de un source_url."""
    parsed = urlparse(source_url)
    params = parse_qs(parsed.query)
    ids = params.get("votaciont", [])
    return ids[0] if ids else None


def backfill_legislature(
    conn: sqlite3.Connection, legislature: str, dates: dict[str, str | None]
) -> dict:
    """UPDATE raw_vote_event con fechas para una legislatura."""
    # Get all VE sin fecha que tengan votaciont en URL
    rows = conn.execute(
        """
        SELECT vote_event_id, source_url FROM raw_vote_event
        WHERE chamber = 'diputados' AND legislature = ? AND vote_date IS NULL
          AND source_url LIKE '%votaciont=%'
    """,
        (legislature,),
    ).fetchall()

    already_has_date = conn.execute(
        """
        SELECT COUNT(*) FROM raw_vote_event
        WHERE chamber = 'diputados' AND legislature = ? AND vote_date IS NOT NULL
    """,
        (legislature,),
    ).fetchone()[0]

    no_votaciont = conn.execute(
        """
        SELECT COUNT(*) FROM raw_vote_event
        WHERE chamber = 'diputados' AND legislature = ? AND vote_date IS NULL
          AND source_url NOT LIKE '%votaciont=%'
    """,
        (legislature,),
    ).fetchone()[0]

    updated = 0
    no_match = 0
    parse_errors = 0

    for ve_id, source_url in rows:
        vid = extract_votacion_id(source_url)
        if not vid:
            no_match += 1
            continue

        fecha_str = dates.get(vid)
        if not fecha_str:
            no_match += 1
            continue

        parsed_date = _parse_spanish_date(fecha_str)
        if not parsed_date:
            parse_errors += 1
            continue

        conn.execute(
            "UPDATE raw_vote_event SET vote_date = ? WHERE vote_event_id = ?",
            (parsed_date.isoformat(), ve_id),
        )
        updated += 1

    return {
        "updated": updated,
        "no_match": no_match,
        "parse_errors": parse_errors,
        "already_has_date": already_has_date,
        "no_votaciont": no_votaciont,
    }


def main():
    config = load_config()
    legislatures = config.get("legislatures", {})

    print("=== Backfill vote_date ===\n", file=sys.stderr)

    total_updated = 0

    with sqlite3.connect(str(DB_PATH)) as conn:
        for leg_name in legislatures:
            # Check if there are VEs needing dates
            count = conn.execute(
                """
                SELECT COUNT(*) FROM raw_vote_event
                WHERE chamber = 'diputados' AND legislature = ? AND vote_date IS NULL
            """,
                (leg_name,),
            ).fetchone()[0]

            if count == 0:
                print(f"{leg_name}: No VEs without dates\n", file=sys.stderr)
                continue

            print(f"{leg_name}: {count} VEs without dates", file=sys.stderr)

            # Fetch index pages
            dates = fetch_index_dates(config, leg_name)
            dates_with_value = {k: v for k, v in dates.items() if v is not None}
            periods = (
                config["legislatures"]
                .get(leg_name, {})
                .get("periods", {})
                .get("diputados_sitl", [])
            )
            print(
                f"  Fetched {len(periods)} periods, "
                f"{len(dates_with_value)}/{len(dates)} votaciones with dates",
                file=sys.stderr,
            )

            # Backfill
            result = backfill_legislature(conn, leg_name, dates)
            print(
                f"  Updated {result['updated']} VE "
                f"({result['no_votaciont']} VE had no votaciont in URL, "
                f"{result['no_match']} no match in index, "
                f"{result['parse_errors']} parse errors)",
                file=sys.stderr,
            )
            print(
                f"  {result['already_has_date']} VE already had dates",
                file=sys.stderr,
            )

            conn.commit()
            total_updated += result["updated"]
            print(file=sys.stderr)

    print(f"=== Total: {total_updated} VE updated ===", file=sys.stderr)


if __name__ == "__main__":
    main()
