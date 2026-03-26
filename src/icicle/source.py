"""Abstract event source for icicle."""

from __future__ import annotations

from abc import ABC
from abc import abstractmethod
from typing import Any


class EventSource(ABC):
    """Abstract base for reading filesystem events."""

    @abstractmethod
    def read(self) -> list[dict[str, Any]]:
        """Return a batch of events. Empty list = no more events."""

    @abstractmethod
    def close(self) -> None:
        """Release resources."""

    @abstractmethod
    def stats(self) -> dict[str, Any]:
        """Return source metrics."""
