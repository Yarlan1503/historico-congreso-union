"""Persistencia idempotente del scraper productivo.

Guarda bodies crudos en filesystem y metadata/estructuras en SQLite.
"""

from __future__ import annotations

import json
import logging
import secrets
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from f2.models import (
    AssetRole,
    Method,
)
from scraper._types import FetchResult, ProcessResult
from shared.persistence_core import (
    insert_raw_vote_casts,
    insert_raw_vote_event,
    insert_source_asset,
    insert_vote_counts,
    insert_vote_event_asset,
)

logger = logging.getLogger(__name__)


class ScraperPersistence:
    """Capa de persistencia idempotente: filesystem + SQLite."""

    def __init__(
        self,
        db_path: Path,
        raw_base_dir: Path,
        run_id: str | None = None,
    ) -> None:
        """Inicializa la persistencia.

        Args:
            db_path: Ruta al archivo SQLite (schema ya inicializado).
            raw_base_dir: Directorio raíz para guardar bodies crudos.
            run_id: Identificador de ejecución; se genera uno si es None.
        """
        if run_id is None:
            ts = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
            run_id = f"{ts}_{secrets.token_hex(4)}"
        self.run_id = run_id
        self.db_path = db_path
        self.raw_base_dir = raw_base_dir

        raw_base_dir.mkdir(parents=True, exist_ok=True)

        self._conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode = WAL")
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._conn.execute("PRAGMA busy_timeout = 5000")
        logger.info("ScraperPersistence inicializada: db=%s raw=%s run_id=%s", db_path, raw_base_dir, run_id)

    # ------------------------------------------------------------------
    # Guardado en filesystem
    # ------------------------------------------------------------------
    def save_raw_asset(self, fetch_result: FetchResult, asset_id: int) -> Path:
        """Guarda el body, headers y metadata cruda en disco.

        Args:
            fetch_result: Resultado de la petición HTTP.
            asset_id: ID del asset en la base de datos.

        Returns:
            Directorio donde se escribieron los archivos.
        """
        asset_dir = self.raw_base_dir / str(asset_id)
        asset_dir.mkdir(parents=True, exist_ok=True)

        (asset_dir / "body.bin").write_bytes(fetch_result.body)
        (asset_dir / "headers.json").write_text(
            json.dumps(dict(fetch_result.headers), ensure_ascii=False),
            encoding="utf-8",
        )

        meta: dict[str, Any] = {
            "url": fetch_result.url,
            "method": fetch_result.method,
            "status_code": fetch_result.status_code,
            "latency_ms": fetch_result.latency_ms,
            "timestamp": fetch_result.timestamp.isoformat(),
            "sha256_body": fetch_result.sha256_body,
            "run_id": self.run_id,
        }
        (asset_dir / "meta.json").write_text(
            json.dumps(meta, ensure_ascii=False, default=str),
            encoding="utf-8",
        )

        if fetch_result.request_payload:
            (asset_dir / "payload.bin").write_bytes(fetch_result.request_payload)

        logger.debug("save_raw_asset: asset_id=%s -> %s", asset_id, asset_dir)
        return asset_dir

    # ------------------------------------------------------------------
    # Persistencia principal (SQLite)
    # ------------------------------------------------------------------
    def persist(self, process_result: ProcessResult, source_tag: str) -> dict[str, Any]:
        """Persiste un ``ProcessResult`` en la base de datos.

        Para clasificaciones distintas de SUCCESS solo se inserta el
        ``source_asset`` a fin de preservar evidencia.

        Args:
            process_result: Resultado del pipeline de procesamiento.
            source_tag: Etiqueta de la fuente que originó el asset.

        Returns:
            Dict JSON-serializable con los IDs insertados y banderas.
        """
        self._conn.execute("BEGIN")
        try:
            fetch = process_result.fetch_result
            asset_dict = self._build_source_asset(fetch, source_tag)

            asset_id, asset_inserted = insert_source_asset(self._conn, asset_dict)

            # Guardar archivos crudos en disco
            raw_dir = self.save_raw_asset(fetch, asset_id)

            # Actualizar raw_body_path con el path real del directorio guardado
            try:
                raw_body_path = str(raw_dir.relative_to(Path.cwd()))
            except ValueError:
                raw_body_path = str(raw_dir)
            self._conn.execute(
                "UPDATE source_asset SET raw_body_path = ? WHERE asset_id = ?",
                (raw_body_path, asset_id),
            )

            result: dict[str, Any] = {
                "asset_id": asset_id,
                "vote_event_id": None,
                "asset_inserted": asset_inserted,
                "event_inserted": False,
                "casts_inserted": 0,
                "counts_inserted": 0,
            }

            if process_result.classification != "SUCCESS":
                logger.info(
                    "persist: classification=%s asset_id=%s (solo asset preservado)",
                    process_result.classification,
                    asset_id,
                )
                self._conn.commit()
                return result

            # --- Vote event ---
            if process_result.vote_event:
                event_dict = process_result.vote_event
            else:
                from scraper.pipeline import _build_vote_event as pipeline_build_vote_event
                event_dict = pipeline_build_vote_event(
                    process_result.fetch_result,
                    process_result.parsed_data or {},
                    source_tag=str(source_tag),
                )
            vote_event_id, event_inserted = insert_raw_vote_event(self._conn, event_dict)
            result["vote_event_id"] = vote_event_id
            result["event_inserted"] = event_inserted

            # --- Link ---
            vote_event_asset = process_result.vote_event_asset or {}
            link_dict = {
                "vote_event_id": vote_event_id,
                "asset_id": asset_id,
                "asset_role": vote_event_asset.get("asset_role", AssetRole.METADATA),
            }
            insert_vote_event_asset(self._conn, link_dict)

            # --- Casts ---
            if process_result.casts:
                for cast in process_result.casts:
                    cast["vote_event_id"] = vote_event_id
                    cast["asset_id"] = asset_id
                casts_inserted = insert_raw_vote_casts(self._conn, process_result.casts)
                result["casts_inserted"] = casts_inserted

            # --- Counts ---
            if process_result.counts:
                for vc in process_result.counts:
                    vc["vote_event_id"] = vote_event_id
                    vc["asset_id"] = asset_id
                counts_inserted = insert_vote_counts(self._conn, process_result.counts)
                result["counts_inserted"] = counts_inserted

            logger.info(
                "persist: asset_id=%s vote_event_id=%s asset_inserted=%s event_inserted=%s casts=%s counts=%s",
                asset_id,
                vote_event_id,
                asset_inserted,
                event_inserted,
                result["casts_inserted"],
                result["counts_inserted"],
            )
            self._conn.commit()
            return result
        except Exception:
            self._conn.rollback()
            raise

    # ------------------------------------------------------------------
    # Builders
    # ------------------------------------------------------------------
    def _build_source_asset(
        self,
        fetch_result: FetchResult,
        source_tag: str,
    ) -> dict[str, Any]:
        """Mapea ``FetchResult`` → dict compatible con ``SourceAsset``."""
        asset_dir = self.raw_base_dir / "{asset_id}"  # placeholder, se resuelve tras insert
        try:
            raw_body_path = asset_dir.relative_to(Path.cwd())
        except ValueError:
            raw_body_path = asset_dir

        request_payload_hash = None
        if fetch_result.request_payload:
            import hashlib

            request_payload_hash = hashlib.sha256(fetch_result.request_payload).hexdigest()

        return {
            "source_tag": source_tag,
            "url": fetch_result.url,
            "method": Method(fetch_result.method.upper()).value,
            "request_payload_hash": request_payload_hash,
            "response_body_hash": fetch_result.sha256_body,
            "response_headers_hash": fetch_result.sha256_headers,
            "status_code": fetch_result.status_code,
            "content_type": fetch_result.headers.get("content-type") or fetch_result.headers.get("Content-Type"),
            "encoding": None,
            "captured_at": fetch_result.timestamp.isoformat(),
            "waf_detected": fetch_result.waf_detected,
            "cache_detected": fetch_result.cache_detected,
            "repetition_num": 1,
            "run_id": self.run_id,
            "raw_body_path": raw_body_path,
        }

    def close(self) -> None:
        """Cierra la conexión a la base de datos."""
        self._conn.close()
        logger.info("ScraperPersistence cerrada")

    def __enter__(self) -> "ScraperPersistence":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
