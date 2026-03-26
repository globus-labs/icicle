"""icicle test configuration — keeps tests isolated from legacy conftest."""

from __future__ import annotations

from typing import Any

from src.icicle.source import EventSource


class InMemoryEventSource(EventSource):
    """Event source for testing — pops pre-loaded batches."""

    def __init__(self, batches: list[list[dict[str, Any]]]) -> None:
        """Initialize with pre-loaded event batches."""
        self._batches = list(batches)

    def read(self) -> list[dict[str, Any]]:
        """Pop and return the next batch, or empty list."""
        if self._batches:
            return self._batches.pop(0)
        return []

    def close(self) -> None:
        """Clear remaining batches."""
        self._batches.clear()

    def stats(self) -> dict[str, Any]:
        """Return empty stats."""
        return {}
