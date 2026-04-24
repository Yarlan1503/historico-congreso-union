#!/usr/bin/env python3
import sqlite3

conn = sqlite3.connect("data/historico.db")
conn.execute("PRAGMA foreign_keys = ON")
result = conn.execute("PRAGMA foreign_keys").fetchone()[0]
print(f"foreign_keys = {result}")
assert result == 1
