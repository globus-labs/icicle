"""Tests for SharedBatchQueue (shared-memory MPSC queue)."""

from __future__ import annotations

import threading

import pytest

from src.icicle.mpsc_queue import SharedBatchQueue

# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


class TestConstruction:
    def test_default_params(self):
        q = SharedBatchQueue()
        try:
            assert q._capacity == 128
            assert q._max_payload == 1_048_576
        finally:
            q.close()
            q.unlink()

    def test_custom_params(self):
        q = SharedBatchQueue(max_batches=4, max_payload_bytes=256)
        try:
            assert q._capacity == 4
            assert q._max_payload == 256
        finally:
            q.close()
            q.unlink()

    def test_invalid_params(self):
        with pytest.raises(ValueError):
            SharedBatchQueue(max_batches=0)
        with pytest.raises(ValueError):
            SharedBatchQueue(max_payload_bytes=-1)


# ---------------------------------------------------------------------------
# Put / Get roundtrip
# ---------------------------------------------------------------------------


class TestPutGet:
    def test_single_batch(self):
        q = SharedBatchQueue(max_batches=4, max_payload_bytes=4096)
        try:
            batch = [{'key': 'value', 'num': 42}]
            q.put(batch)
            result = q.get()
            assert result == batch
        finally:
            q.close()
            q.unlink()

    def test_multiple_batches(self):
        q = SharedBatchQueue(max_batches=8, max_payload_bytes=4096)
        try:
            batches = [[{'i': i, 'data': f'batch-{i}'}] for i in range(5)]
            for b in batches:
                q.put(b)
            for b in batches:
                assert q.get() == b
        finally:
            q.close()
            q.unlink()

    def test_empty_batch(self):
        q = SharedBatchQueue(max_batches=4, max_payload_bytes=4096)
        try:
            q.put([])
            assert q.get() == []
        finally:
            q.close()
            q.unlink()

    def test_large_batch(self):
        q = SharedBatchQueue(max_batches=4, max_payload_bytes=65536)
        try:
            batch = [{'k': f'v-{i}', 'data': 'x' * 100} for i in range(100)]
            q.put(batch)
            result = q.get()
            assert len(result) == 100
            assert result[0]['k'] == 'v-0'
        finally:
            q.close()
            q.unlink()


# ---------------------------------------------------------------------------
# Sentinel handling
# ---------------------------------------------------------------------------


class TestSentinel:
    def test_none_sentinel(self):
        q = SharedBatchQueue(max_batches=4, max_payload_bytes=4096)
        try:
            q.put(None)
            assert q.get() is None
        finally:
            q.close()
            q.unlink()

    def test_batch_then_sentinel(self):
        q = SharedBatchQueue(max_batches=4, max_payload_bytes=4096)
        try:
            q.put([{'a': 1}])
            q.put(None)
            assert q.get() == [{'a': 1}]
            assert q.get() is None
        finally:
            q.close()
            q.unlink()


# ---------------------------------------------------------------------------
# Overflow
# ---------------------------------------------------------------------------


class TestOverflow:
    def test_payload_too_large_raises(self):
        q = SharedBatchQueue(max_batches=4, max_payload_bytes=64)
        try:
            big_batch = [{'data': 'x' * 100}]
            with pytest.raises(ValueError, match='exceeds slot size'):
                q.put(big_batch)
        finally:
            q.close()
            q.unlink()


# ---------------------------------------------------------------------------
# Multi-producer correctness
# ---------------------------------------------------------------------------


class TestMultiProducer:
    def test_two_producers(self):
        q = SharedBatchQueue(max_batches=32, max_payload_bytes=4096)
        try:
            n_per_producer = 10
            results: list = []
            lock = threading.Lock()

            def produce(pid: int) -> None:
                for i in range(n_per_producer):
                    q.put([{'pid': pid, 'seq': i}])
                q.put(None)

            def consume() -> None:
                done = 0
                while done < 2:
                    batch = q.get()
                    if batch is None:
                        done += 1
                    else:
                        with lock:
                            results.extend(batch)

            t1 = threading.Thread(target=produce, args=(0,))
            t2 = threading.Thread(target=produce, args=(1,))
            tc = threading.Thread(target=consume)

            tc.start()
            t1.start()
            t2.start()

            t1.join(timeout=5)
            t2.join(timeout=5)
            tc.join(timeout=5)

            assert len(results) == 2 * n_per_producer
            p0 = [r for r in results if r['pid'] == 0]
            p1 = [r for r in results if r['pid'] == 1]
            assert len(p0) == n_per_producer
            assert len(p1) == n_per_producer
        finally:
            q.close()
            q.unlink()


# ---------------------------------------------------------------------------
# Timing metrics
# ---------------------------------------------------------------------------


class TestTimingMetrics:
    def test_metrics_update_after_put(self):
        q = SharedBatchQueue(max_batches=4, max_payload_bytes=4096)
        try:
            assert q._slot_hold_count == 0
            q.put([{'x': 1}])
            assert q._slot_hold_count == 1
            assert q._slot_hold_total_ns > 0
            assert q._slot_hold_max_ns > 0
            q.get()
        finally:
            q.close()
            q.unlink()


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------


class TestCleanup:
    def test_close_and_unlink(self):
        q = SharedBatchQueue(max_batches=4, max_payload_bytes=256)
        q.close()
        q.unlink()
        assert q._unlinked is True

    def test_double_unlink_safe(self):
        q = SharedBatchQueue(max_batches=4, max_payload_bytes=256)
        q.close()
        q.unlink()
        q.unlink()  # should not raise
