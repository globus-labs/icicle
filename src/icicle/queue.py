"""Batch queue implementations for icicle."""

from __future__ import annotations

import queue
import threading
from abc import ABC
from abc import abstractmethod
from typing import Any


class BatchQueue(ABC):
    """Abstract batch queue for producer-to-consumer event passing."""

    @abstractmethod
    def put(self, batch: list[dict[str, Any]] | None) -> None:
        """Put a batch or None sentinel."""

    @abstractmethod
    def get(
        self,
        timeout: float | None = None,
    ) -> list[dict[str, Any]] | None:
        """Get next batch. Returns None for sentinel.

        Raises queue.Empty on timeout.
        """

    @abstractmethod
    def close(self) -> None:
        """Release resources."""

    def stats(self) -> dict[str, Any]:
        """Return queue metrics. Default: empty dict."""
        return {}


class StdlibQueue(BatchQueue):
    """Wraps queue.Queue for thread-safe FIFO."""

    def __init__(self, maxsize: int = 128) -> None:
        """Initialize with a bounded queue."""
        self._q: queue.Queue[list[dict[str, Any]] | None] = queue.Queue(
            maxsize=maxsize,
        )

    def put(self, batch: list[dict[str, Any]] | None) -> None:
        """Put a batch into the queue."""
        self._q.put(batch)

    def get(
        self,
        timeout: float | None = None,
    ) -> list[dict[str, Any]] | None:
        """Get a batch, raising queue.Empty on timeout."""
        return self._q.get(timeout=timeout)

    def close(self) -> None:
        """No-op for stdlib queue."""


class RingBufferQueue(BatchQueue):
    """Fast MPSC ring buffer for threads. No serialization overhead."""

    def __init__(self, capacity: int = 128) -> None:
        """Initialize ring buffer with the given capacity."""
        self._buf: list[list[dict[str, Any]] | None] = [None] * capacity
        self._capacity = capacity
        self._head = 0
        self._tail = 0
        self._slots_available = threading.Semaphore(capacity)
        self._items_available = threading.Semaphore(0)
        self._tail_lock = threading.Lock()
        self._put_count = 0
        self._get_count = 0

    def put(self, batch: list[dict[str, Any]] | None) -> None:
        """Put a batch into the ring buffer."""
        self._slots_available.acquire()
        with self._tail_lock:
            idx = self._tail
            self._tail = (idx + 1) % self._capacity
            self._buf[idx] = batch
        self._put_count += 1
        self._items_available.release()

    def get(
        self,
        timeout: float | None = None,
    ) -> list[dict[str, Any]] | None:
        """Get a batch from the ring buffer."""
        if not self._items_available.acquire(timeout=timeout):
            raise queue.Empty
        idx = self._head
        self._head = (idx + 1) % self._capacity
        batch = self._buf[idx]
        self._buf[idx] = None  # allow GC
        self._slots_available.release()
        self._get_count += 1
        return batch

    def close(self) -> None:
        """No-op for in-process ring buffer."""

    def stats(self) -> dict[str, Any]:
        """Return put/get counts."""
        return {
            'put_count': self._put_count,
            'get_count': self._get_count,
        }
