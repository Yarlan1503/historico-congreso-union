"""Stub de persistencia para modo dry-run: solo loguea, no toca DB ni disco."""

from __future__ import annotations

import logging
import secrets
from datetime import UTC, datetime
from typing import Any

logger = logging.getLogger(__name__)


class DryRunPersistence:
    """Stub de persistencia para modo dry-run."""

    def __init__(self, run_id: str | None = None) -> None:
        if run_id is None:
            ts = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
            run_id = f"{ts}_{secrets.token_hex(4)}"
        self.run_id = run_id

    def persist(self, process_result: Any, source_tag: Any) -> dict[str, Any]:
        logger.info(
            "[DRY-RUN] persist: %s (source=%s)",
            process_result.classification,
            source_tag,
        )
        return {}

    def __enter__(self) -> DryRunPersistence:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        pass
