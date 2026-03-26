"""Unified event kinds for icicle."""

from __future__ import annotations

from enum import StrEnum
from enum import unique


@unique
class EventKind(StrEnum):
    """Unified event kinds across all backends (fswatch, Lustre, GPFS)."""

    CREATED = 'CREATED'
    REMOVED = 'REMOVED'
    RENAMED = 'RENAMED'
    MODIFIED = 'MODIFIED'
