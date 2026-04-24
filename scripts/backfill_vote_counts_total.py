#!/usr/bin/env python3
"""Backfill de vote_counts.total para filas existentes con NULL."""
import sqlite3
import sys
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "historico.db"

conn = sqlite3.connect(str(DB_PATH))
conn.execute("PRAGMA foreign_keys = ON")
conn.execute("PRAGMA busy_timeout = 5000")

cursor = conn.execute(
    """
    UPDATE vote_counts
    SET total = COALESCE(a_favor, 0) + COALESCE(en_contra, 0) 
               + COALESCE(abstencion, 0) + COALESCE(ausente, 0) 
               + COALESCE(novoto, 0) + COALESCE(presente, 0)
    WHERE total IS NULL
    """
)
updated = cursor.rowcount
conn.commit()
conn.close()
print(f"✅ vote_counts.total backfill: {updated} filas actualizadas")
