"""Simple performance benchmarks for icicle core components."""

from __future__ import annotations

import time

from src.icicle.batch import BatchProcessor
from src.icicle.events import EventKind
from src.icicle.fswatch_events import STAT_REDUCTION_RULES
from src.icicle.state import StateManager


def ev(path: str, kind: EventKind, is_dir: bool = False) -> dict:
    return {'path': path, 'kind': kind, 'ts': 0.0, 'is_dir': is_dir}


class FakeStat:
    st_size = 100
    st_uid = 1000
    st_gid = 1000
    st_mode = 0o100644
    st_atime = 1000
    st_mtime = 2000
    st_ctime = 3000


# ---------------------------------------------------------------------------
# BatchProcessor benchmarks
# ---------------------------------------------------------------------------


class TestBatchBenchmarks:
    def test_batch_throughput_10k(self):
        """10k events to BatchProcessor, unique paths."""
        events = [
            ev(f'/file_{i}.txt', EventKind.CREATED) for i in range(10_000)
        ]
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        t0 = time.perf_counter()
        bp.add_events(events)
        bp.flush()
        elapsed = time.perf_counter() - t0
        rate = 10_000 / elapsed
        print(
            f'\n  batch_throughput_10k: {rate:,.0f} events/sec ({elapsed * 1000:.1f}ms)',
        )
        assert rate > 50_000, f'Too slow: {rate:.0f} events/sec'

    def test_batch_throughput_100k(self):
        """100k events, unique paths."""
        events = [
            ev(f'/file_{i}.txt', EventKind.CREATED) for i in range(100_000)
        ]
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        t0 = time.perf_counter()
        bp.add_events(events)
        bp.flush()
        elapsed = time.perf_counter() - t0
        rate = 100_000 / elapsed
        print(
            f'\n  batch_throughput_100k: {rate:,.0f} events/sec ({elapsed * 1000:.1f}ms)',
        )
        assert rate > 50_000, f'Too slow: {rate:.0f} events/sec'

    def test_batch_reduction_throughput(self):
        """100k events all same path → heavy reduction."""
        events = [ev('/same.txt', EventKind.MODIFIED) for _ in range(100_000)]
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        t0 = time.perf_counter()
        bp.add_events(events)
        batch = bp.flush()
        elapsed = time.perf_counter() - t0
        rate = 100_000 / elapsed
        print(
            f'\n  batch_reduction_100k: {rate:,.0f} events/sec ({elapsed * 1000:.1f}ms)',
        )
        assert len(batch) == 1
        assert rate > 100_000, f'Too slow: {rate:.0f} events/sec'


# ---------------------------------------------------------------------------
# StateManager benchmarks
# ---------------------------------------------------------------------------


class TestStateBenchmarks:
    def test_state_create_throughput_10k(self):
        """10k CREATED events through StateManager."""
        events = [
            ev(f'/file_{i}.txt', EventKind.CREATED) for i in range(10_000)
        ]
        sm = StateManager()
        t0 = time.perf_counter()
        sm.process_events(events)
        elapsed = time.perf_counter() - t0
        rate = 10_000 / elapsed
        print(
            f'\n  state_create_10k: {rate:,.0f} events/sec ({elapsed * 1000:.1f}ms)',
        )
        assert rate > 10_000, f'Too slow: {rate:.0f} events/sec'
        assert len(sm.files) == 10_000

    def test_state_rename_deep_tree(self):
        """Create tree with 1000 files under /a/, rename /a/ → /b/."""
        sm = StateManager()
        # Create parent dir
        sm.process_events([ev('/a', EventKind.CREATED, is_dir=True)])
        # Create 1000 files
        creates = [
            ev(f'/a/file_{i}.txt', EventKind.CREATED) for i in range(1_000)
        ]
        sm.process_events(creates)
        sm.to_update.clear()

        # Rename
        t0 = time.perf_counter()
        sm.process_events(
            [
                ev('/a', EventKind.RENAMED, is_dir=True),
                ev('/b', EventKind.RENAMED, is_dir=True),
            ],
        )
        elapsed = time.perf_counter() - t0
        print(f'\n  state_rename_1k_files: {elapsed * 1000:.2f}ms')
        assert elapsed < 0.5, f'Too slow: {elapsed * 1000:.1f}ms'
        assert '/b' in sm.files
        assert '/b/file_0.txt' in sm.files
        assert '/b/file_999.txt' in sm.files
        assert len(sm.to_update) == 1001  # dir + 1000 files

    def test_emit_throughput(self):
        """Emit 10k updates with mocked os.stat."""
        sm = StateManager(stat_fn=lambda p: FakeStat())
        events = [
            ev(f'/file_{i}.txt', EventKind.CREATED) for i in range(10_000)
        ]
        sm.process_events(events)

        t0 = time.perf_counter()
        changes = sm.emit()
        elapsed = time.perf_counter() - t0

        rate = len(changes) / elapsed
        print(f'\n  emit_10k: {rate:,.0f} events/sec ({elapsed * 1000:.1f}ms)')
        assert len(changes) == 10_000
        assert rate > 10_000, f'Too slow: {rate:.0f} events/sec'


# ---------------------------------------------------------------------------
# Full pipeline benchmark
# ---------------------------------------------------------------------------


class TestPipelineBenchmark:
    def test_full_pipeline_10k(self):
        """Full pipeline: 10k events → batch → state → emit."""
        events = [
            ev(f'/file_{i}.txt', EventKind.CREATED) for i in range(10_000)
        ]
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        sm = StateManager(stat_fn=lambda p: FakeStat())

        t0 = time.perf_counter()
        bp.add_events(events)
        coalesced = bp.flush()
        sm.process_events(coalesced)
        changes = sm.emit()
        elapsed = time.perf_counter() - t0

        rate = 10_000 / elapsed
        print(
            f'\n  full_pipeline_10k: {rate:,.0f} events/sec ({elapsed * 1000:.1f}ms)',
        )
        assert len(changes) == 10_000
        assert rate > 5_000, f'Too slow: {rate:.0f} events/sec'
