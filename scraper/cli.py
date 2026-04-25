"""CLI del scraper productivo."""

from __future__ import annotations

import argparse
import json
import logging
import logging.handlers
import secrets
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from f2.db_init import init_db
from scraper.dry_run import DryRunPersistence
from scraper.engine import HTTPScraperEngine
from scraper.exporter.snapshot import export_snapshot
from scraper.persistence import ScraperPersistence
from scraper.sources.gaceta import scrape_gaceta
from scraper.sources.gaceta_lxvi import scrape_gaceta_lxvi
from scraper.sources.senado import scrape_senado
from scraper.sources.senado_historico import scrape_senado_historico
from scraper.sources.senado_lxvi import scrape_senado_lxvi
from scraper.sources.sitl import scrape_sitl
from scraper.sources.sitl_lxvi import scrape_sitl_lxvi

logger = logging.getLogger(__name__)

# PROJECT_ROOT se usa solo para defaults de CLI args
PROJECT_ROOT = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# Helpers de parsing de argumentos
# ---------------------------------------------------------------------------

def _parse_range(value: str | None) -> range | None:
    if value is None:
        return None
    try:
        start, end = value.split("-", 1)
        return range(int(start), int(end) + 1)
    except (ValueError, TypeError) as exc:
        raise argparse.ArgumentTypeError(
            f"Rango inválido '{value}': esperado formato 'inicio-fin'"
        ) from exc


def _parse_since(value: str | None) -> Any:
    if value is None:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Fecha inválida '{value}': esperado YYYY-MM-DD"
        ) from exc


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="CLI del scraper productivo de historico-congreso-union"
    )
    parser.add_argument(
        "--source",
        type=str,
        default="all",
        choices=[
            "sitl", "gaceta", "senado", "senado_historico",
            "sitl_lxvi", "gaceta_lxvi", "senado_lxvi", "all",
        ],
        help="Fuente a scrapear",
    )
    parser.add_argument(
        "--legislature",
        type=str,
        default="LXVI",
        help="Legislatura para fuentes paramétricas (LXVI, LXV, LXIV, LXIII, LXII)",
    )
    parser.add_argument(
        "--since",
        type=_parse_since,
        default=None,
        help="Fecha límite inferior (YYYY-MM-DD); votaciones anteriores se omiten",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=PROJECT_ROOT / "scraper" / "config.toml",
        help="Archivo de configuración TOML",
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=PROJECT_ROOT / "data" / "historico.db",
        help="Ruta a la base SQLite",
    )
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=PROJECT_ROOT / "data" / "raw",
        help="Directorio para bodies crudos",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Solo valida y reporta; no inserta en DB ni escribe raw",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Nivel de logging",
    )
    parser.add_argument(
        "--max-votaciones",
        type=int,
        default=None,
        help="Límite de votaciones por periodo (solo SITL)",
    )
    parser.add_argument(
        "--tabla-range",
        type=_parse_range,
        default=None,
        help="Rango de tablas Gaceta (ej. 1-200)",
    )
    parser.add_argument(
        "--id-range",
        type=_parse_range,
        default=None,
        help="Rango de IDs Senado (ej. 1-5000)",
    )
    parser.add_argument(
        "--camara",
        type=str,
        default=None,
        choices=["D", "S"],
        help="Cámara para exportar (D=Diputados, S=Senado). Solo para --export.",
    )
    parser.add_argument(
        "--export",
        action="store_true",
        help="Modo export: produce snapshot para popolo-congreso-union en vez de scraping",
    )

    args = parser.parse_args(argv)

    log_dir = PROJECT_ROOT / "data" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "scraper.log"

    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)

    file_handler = logging.handlers.RotatingFileHandler(
        log_path, maxBytes=5_000_000, backupCount=3, encoding="utf-8"
    )
    file_handler.setFormatter(formatter)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        handlers=[stream_handler, file_handler],
    )

    # ------------------------------------------------------------------
    # 0. Modo export
    # ------------------------------------------------------------------
    if args.export:
        if args.camara is None:
            parser.error("--camara es requerido con --export")
        camara = args.camara
        legislature = args.legislature
        chamber_source = "diputados" if camara == "D" else "senado"

        snapshots_base = PROJECT_ROOT / "snapshots"
        raw_dir = args.raw_dir

        logger.info("Iniciando export: camara=%s legislature=%s", camara, legislature)
        result = export_snapshot(
            db_path=args.db_path,
            raw_dir=raw_dir,
            output_base=snapshots_base,
            chamber_source=chamber_source,
            legislature=legislature,
        )
        logger.info("Export completado: %s", json.dumps(result, indent=2, default=str))
        return 0

    # ------------------------------------------------------------------
    # 1. Inicializar DB si no existe
    # ------------------------------------------------------------------
    try:
        if not args.db_path.exists():
            logger.info(
                "Base de datos no encontrada en %s; inicializando...",
                args.db_path,
            )
            init_db(args.db_path)
            logger.info("Base de datos inicializada.")
    except Exception as exc:
        logger.critical("No se pudo inicializar la base de datos: %s", exc)
        return 1

    # ------------------------------------------------------------------
    # 2. Validar config
    # ------------------------------------------------------------------
    if not args.config.exists():
        logger.critical("Archivo de configuración no encontrado: %s", args.config)
        return 1

    since = args.since
    tabla_range = args.tabla_range
    id_range = args.id_range
    legislature = args.legislature

    # ------------------------------------------------------------------
    # 3. Generar run_id compartido para toda la ejecución
    # ------------------------------------------------------------------
    ts = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    run_id = f"{ts}_{secrets.token_hex(4)}"

    # ------------------------------------------------------------------
    # 4. Determinar fuentes a ejecutar
    # ------------------------------------------------------------------
    if args.source == "all":
        sources_to_run = ["sitl", "gaceta", "senado"]
    else:
        sources_to_run = [args.source]

    por_fuente: dict[str, dict[str, Any]] = {}

    # ------------------------------------------------------------------
    # 5. Ejecutar scraping
    # ------------------------------------------------------------------
    try:
        if args.source in ("senado", "senado_historico"):
            from scraper.senado_client import SenadoAntiWAFClient
            engine_cm = SenadoAntiWAFClient(config_path=args.config)
        else:
            engine_cm = HTTPScraperEngine(config_path=args.config)
        with engine_cm as engine:
            if args.dry_run:
                persistence: ScraperPersistence | DryRunPersistence = (
                    DryRunPersistence(run_id=run_id)
                )
            else:
                persistence = ScraperPersistence(
                    db_path=args.db_path,
                    raw_base_dir=args.raw_dir,
                    run_id=run_id,
                )

            with persistence:
                for source in sources_to_run:
                    logger.info("Iniciando fuente: %s", source)
                    try:
                        if source == "sitl":
                            stats = scrape_sitl(
                                engine,
                                persistence,
                                legislature=legislature,
                                since=since,
                                max_votaciones=args.max_votaciones,
                            )
                        elif source == "gaceta":
                            stats = scrape_gaceta(
                                engine,
                                persistence,
                                legislature=legislature,
                                since=since,
                                tabla_range=tabla_range,
                            )
                        elif source == "senado":
                            stats = scrape_senado(
                                engine,
                                persistence,
                                legislature=legislature,
                                since=since,
                                id_range=id_range,
                            )
                        elif source == "senado_historico":
                            stats = scrape_senado_historico(
                                engine,
                                persistence,
                                legislature=legislature,
                                since=since,
                            )
                        elif source == "sitl_lxvi":
                            stats = scrape_sitl_lxvi(
                                engine,
                                persistence,
                                since=since,
                                max_votaciones=args.max_votaciones,
                            )
                        elif source == "gaceta_lxvi":
                            stats = scrape_gaceta_lxvi(
                                engine,
                                persistence,
                                since=since,
                                tabla_range=tabla_range,
                            )
                        elif source == "senado_lxvi":
                            stats = scrape_senado_lxvi(
                                engine,
                                persistence,
                                since=since,
                                id_range=id_range,
                            )
                        else:
                            logger.error("Fuente desconocida: %s", source)
                            continue
                        por_fuente[source] = stats
                        logger.info("Fuente %s completada.", source)
                    except Exception as exc:
                        logger.exception(
                            "Error fatal ejecutando fuente %s", source
                        )
                        por_fuente[source] = {
                            "source": source,
                            "error_fatal": str(exc),
                        }
    except Exception as exc:
        logger.critical("Error fatal en el motor de scraping: %s", exc)
        return 1

    # ------------------------------------------------------------------
    # 6. Reporte final agregado
    # ------------------------------------------------------------------
    totals = {
        "assets_insertados": 0,
        "assets_skipped": 0,
        "vote_events_insertados": 0,
        "vote_events_existentes": 0,
        "casts_insertados": 0,
        "counts_insertados": 0,
        "waf_detectados": 0,
        "indeterminates": 0,
        "errores": 0,
    }

    for stats in por_fuente.values():
        if "error_fatal" in stats:
            continue
        totals["assets_insertados"] += stats.get("assets_insertados", 0)
        totals["assets_skipped"] += stats.get("assets_skipped", 0)
        totals["vote_events_insertados"] += stats.get(
            "vote_events_insertados", 0
        )
        totals["vote_events_existentes"] += stats.get(
            "vote_events_existentes", 0
        )
        totals["casts_insertados"] += stats.get("casts_insertados", 0)
        totals["counts_insertados"] += stats.get("counts_insertados", 0)
        totals["waf_detectados"] += stats.get("waf_detectados", 0)
        totals["indeterminates"] += stats.get("indeterminates", 0)
        totals["errores"] += len(stats.get("errores", []))

    report = {
        "run_id": run_id,
        "dry_run": args.dry_run,
        "legislature": legislature,
        "since": str(since) if since else None,
        "sources": sources_to_run,
        "totals": totals,
        "por_fuente": por_fuente,
    }

    logger.info("Reporte final: %s", json.dumps(report, indent=2, default=str))

    if any("error_fatal" in s for s in por_fuente.values()):
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
