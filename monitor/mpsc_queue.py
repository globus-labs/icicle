"""Shared-memory multi-producer, single-consumer batch queue."""

from __future__ import annotations

import os
import struct
import time
from multiprocessing import Lock
from multiprocessing import Semaphore
from multiprocessing import Value
from multiprocessing.shared_memory import SharedMemory
from typing import Any

import orjson


class SharedBatchQueue:  # TODO: rename
    """Minimal MPSC shared-memory queue."""

    # header: <I B 3x -> LE, 4B length, 1B flags, 3B pad (8 bytes total)
    _HDR = struct.Struct('<I B 3x')

    def __init__(
        self,
        max_batches: int = 128,
        max_payload_bytes: int = 1_048_576,
    ) -> None:
        """Allocate shared memory for the queue with the provided limits."""
        if max_batches <= 0 or max_payload_bytes <= 0:
            raise ValueError(
                'max_batches and max_payload_bytes must be positive',
            )

        self._n = max_batches
        self._max_payload = max_payload_bytes
        self._slot_size = self._HDR.size + self._max_payload

        self._shm = SharedMemory(create=True, size=self._n * self._slot_size)
        self._buf = self._shm.buf

        # Capacity and item semaphores
        self._slots_available = Semaphore(self._n)
        self._items_available = Semaphore(0)

        # MPSC: producers coordinate with a single tail lock
        self._tail_lock = Lock()
        self._head = Value('I', 0, lock=False)
        self._tail = Value('I', 0, lock=False)

        # Ownership for unlink
        self._owner_pid = os.getpid()
        self._unlinked = False

        # Simple timing metrics (same names as your original code)
        self._slot_hold_total_ns = 0
        self._slot_hold_max_ns = 0
        self._slot_hold_count = 0

    # ---------- Public API ----------
    def put(self, batch: list[dict[str, Any]] | None) -> None:
        """Put a batch (list of dicts) or None (sentinel)."""
        if batch is None:
            self._put_bytes(None)
        else:
            self._put_bytes(orjson.dumps(batch))

    def get(self) -> list[dict[str, Any]] | None:
        """Get the next batch, or None (sentinel)."""
        data = self._get_bytes()
        if data is None:
            return None
        return orjson.loads(data)

    def close(self) -> None:
        """Detach from the shared memory block."""
        self._shm.close()

    def unlink(self) -> None:
        """Remove the shared memory block if we still own it."""
        if not self._unlinked and os.getpid() == self._owner_pid:
            self._shm.unlink()
            self._unlinked = True

    # ---------- Internal helpers ----------
    def _slot_base(self, idx: int) -> int:
        return idx * self._slot_size

    def _put_bytes(self, payload: bytes | None) -> None:
        sentinel = payload is None
        length = 0 if sentinel else len(payload)  # type: ignore[arg-type]
        if length > self._max_payload:
            raise ValueError(
                'batch payload exceeds slot size '
                f'({length} > {self._max_payload})',
            )

        self._slots_available.acquire()
        t0 = time.perf_counter_ns()
        try:
            with self._tail_lock:
                idx = self._tail.value
                self._tail.value = (idx + 1) % self._n

                base = self._slot_base(idx)
                hdr_off = base
                pay_off = base + self._HDR.size

                # 1) write payload first (if not sentinel)
                if not sentinel and length:
                    self._buf[pay_off : pay_off + length] = payload  # type: ignore[assignment]

                # 2) publish header (length + flags)
                flags = 1 if sentinel else 0
                self._HDR.pack_into(self._buf, hdr_off, length, flags)
        except Exception:
            # return capacity on failure
            self._slots_available.release()
            raise

        # timing metrics
        dt = time.perf_counter_ns() - t0
        self._slot_hold_total_ns += dt
        self._slot_hold_max_ns = max(self._slot_hold_max_ns, dt)
        self._slot_hold_count += 1

        # 3) signal item available (only after header is fully written)
        self._items_available.release()

    def _get_bytes(self) -> bytes | None:
        self._items_available.acquire()

        idx = self._head.value
        self._head.value = (idx + 1) % self._n

        base = self._slot_base(idx)
        hdr_off = base
        pay_off = base + self._HDR.size

        length, flags = self._HDR.unpack_from(self._buf, hdr_off)
        sentinel = (flags & 1) == 1

        if sentinel:
            # free the slot and return sentinel
            self._slots_available.release()
            return None

        # Copy bytes before freeing the slot so producers cannot overwrite it
        data = self._buf[pay_off : pay_off + length].tobytes()

        # free the slot
        self._slots_available.release()
        return data


__all__ = ['SharedBatchQueue']
