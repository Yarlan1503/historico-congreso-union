# Historico Congreso Union

Scraper y base de datos de datos crudos de votaciones históricas del Congreso de la Unión de México (Cámara de Diputados y Senado de la República).

## Alcance

- **Scrapeo** de fuentes oficiales: SITL/INFOPAL, Gaceta Parlamentaria, Senado.gob.mx.
- **Preservación** de evidencia cruda (body, headers, status, hash, request params) en filesystem + SQLite.
- **Schema** relacional mínimo: `source_asset`, `raw_vote_event`, `raw_vote_cast`, `vote_counts`.
- **QA** via notebooks Marimo ejecutables.
- **No análisis, no publicación**: este repo se limita a adquirir y almacenar datos crudos con trazabilidad.

## Stack

- [Astral](https://astral.sh/) (`uv`, `ruff`) — entorno y lint
- Python 3.12+
- SQLite (WAL mode, FK ON)
- [Marimo](https://marimo.io/) — notebooks de QA/inspección
- `httpx` — HTTP engine con sesiones, delays, rotación UA
- `pydantic` — validación de modelos

## Estructura

```
.
├── f1/                     # experimental: cartografía empírica, parsers, packets
├── f2/                     # schema productivo SQLite + ingestión
├── scraper/                # scraper productivo: engine, pipeline, persistence, sources
├── shared/                 # módulos compartidos (persistence_core, transform_bridge)
├── notebooks/              # Marimo QA notebooks
├── tests/                  # pytest mínimo
├── data/                   # SQLite + raw artifacts (ignorado por git)
└── LICENSE                 # GNU GPL v3
```

## Datos

El directorio `data/` está excluido del control de versiones. Contiene:
- `data/historico.db` — base SQLite productiva.
- `data/raw/{asset_id}/` — artefactos crudos (body.bin, headers.json, meta.json).
- `data/logs/scraper.log` — logs de ejecución.

## Disclaimer legal

Este proyecto recopila información de fuentes públicas oficiales con fines de transparencia y análisis académico. No evade mecanismos de protección ni realiza accesos no autorizados. Las respuestas bloqueadas o indeterminadas se preservan como evidencia negativa.

## Licencia

GNU General Public License v3.0 — ver [LICENSE](LICENSE).

## Autor

Nolan / CachorroInk
