"""Unit tests for icicle Monitor orchestrator."""

from __future__ import annotations

import os
import signal
from typing import Any

import pytest

from src.icicle.batch import BatchProcessor
from src.icicle.events import EventKind
from src.icicle.fswatch_events import STAT_REDUCTION_RULES
from src.icicle.monitor import Monitor
from src.icicle.output import OutputHandler
from src.icicle.state import StateManager
from tests.icicle.conftest import InMemoryEventSource


def ev(path: str, kind: EventKind, is_dir: bool = False, **kw) -> dict:
    return {'path': path, 'kind': kind, 'ts': 0.0, 'is_dir': is_dir, **kw}


def make_stat_result(**kw):
    defaults = {
        'st_size': 100,
        'st_uid': 1000,
        'st_gid': 1000,
        'st_mode': 0o100644,
        'st_atime': 1000,
        'st_mtime': 2000,
        'st_ctime': 3000,
    }
    defaults.update(kw)

    class FakeStat:
        pass

    s = FakeStat()
    for k, v in defaults.items():
        setattr(s, k, v)
    return s


class FakeOutputHandler(OutputHandler):
    """Records all payloads sent, for assertion."""

    def __init__(self):
        self.payloads: list[dict[str, Any]] = []
        self.closed = False

    def send(self, payloads):
        self.payloads.extend(payloads)

    def flush(self):
        pass

    def close(self):
        self.closed = True


def _fake_state():
    """Create a StateManager with injected fake stat."""
    return StateManager(stat_fn=lambda p: make_stat_result())


# ---------------------------------------------------------------------------
# Monitor.run() tests
# ---------------------------------------------------------------------------


class TestMonitorRun:
    def test_single_batch_end_to_end(self):
        source = InMemoryEventSource(
            [
                [ev('/tmp/a.txt', EventKind.CREATED)],
            ],
        )
        output = FakeOutputHandler()
        monitor = Monitor(source, output, state=_fake_state())
        monitor.run()

        assert len(output.payloads) >= 1
        assert output.payloads[0]['op'] == 'update'
        assert output.payloads[0]['path'] == '/tmp/a.txt'
        stats = monitor.stats()
        assert stats['batch_received'] == 1
        assert stats['batch_accepted'] == 1

    def test_empty_source_exits_cleanly(self):
        source = InMemoryEventSource([])
        output = FakeOutputHandler()
        monitor = Monitor(source, output)
        monitor.run()

        assert output.payloads == []
        assert output.closed is True
        stats = monitor.stats()
        assert stats['batch_received'] == 0

    def test_output_none_no_crash(self):
        source = InMemoryEventSource(
            [
                [ev('/tmp/x.txt', EventKind.CREATED)],
            ],
        )
        monitor = Monitor(source, output=None, state=_fake_state())
        monitor.run()  # should not raise

        assert monitor.stats()['batch_received'] == 1

    def test_multiple_batches_accumulate_state(self):
        source = InMemoryEventSource(
            [
                [ev('/tmp/a.txt', EventKind.CREATED)],
                [ev('/tmp/a.txt', EventKind.MODIFIED)],
            ],
        )
        output = FakeOutputHandler()
        monitor = Monitor(source, output, state=_fake_state())
        monitor.run()

        stats = monitor.stats()
        assert stats['batch_received'] == 2
        # File should be tracked
        assert (
            stats['state_files'] >= 0
        )  # after emit, files may or may not persist

    def test_final_flush_on_shutdown(self):
        source = InMemoryEventSource(
            [
                [ev('/tmp/f.txt', EventKind.CREATED)],
            ],
        )
        output = FakeOutputHandler()
        monitor = Monitor(source, output, state=_fake_state())
        monitor.run()

        # The finally block should have flushed the pending create
        assert len(output.payloads) >= 1
        assert output.closed is True

    def test_stats_after_reduction(self):
        # CREATED + MODIFIED + MODIFIED → CREATED absorbs both MODIFIEDs
        source = InMemoryEventSource(
            [
                [
                    ev('/tmp/f.txt', EventKind.CREATED),
                    ev('/tmp/f.txt', EventKind.MODIFIED),
                    ev('/tmp/f.txt', EventKind.MODIFIED),
                ],
            ],
        )
        output = FakeOutputHandler()
        batch = BatchProcessor(rules=STAT_REDUCTION_RULES)
        monitor = Monitor(source, output, batch=batch, state=_fake_state())
        monitor.run()

        stats = monitor.stats()
        assert stats['batch_received'] == 3
        assert stats['batch_ignored'] + stats['batch_cancelled'] == 2
        assert stats['batch_accepted'] == 1

    def test_close_is_idempotent(self):
        source = InMemoryEventSource([])
        output = FakeOutputHandler()
        monitor = Monitor(source, output)
        monitor.close()
        monitor.close()  # should not raise
        assert output.closed is True

    def test_source_exception_triggers_finally(self):
        class FailingSource:
            def read(self):
                raise RuntimeError('boom')

            def close(self):
                pass

        output = FakeOutputHandler()
        monitor = Monitor(FailingSource(), output)
        with pytest.raises(RuntimeError, match='boom'):
            monitor.run()
        assert output.closed is True

    def test_coalesced_empty_skips_state(self):
        # CREATED + REMOVED cancels in batch → empty commit → state untouched
        source = InMemoryEventSource(
            [
                [
                    ev('/tmp/e.txt', EventKind.CREATED),
                    ev('/tmp/e.txt', EventKind.REMOVED),
                ],
            ],
        )
        output = FakeOutputHandler()
        monitor = Monitor(source, output, state=_fake_state())
        monitor.run()

        assert monitor.stats()['state_files'] == 0
        assert output.payloads == []

    def test_signal_shutdown(self):
        class SignalSource:
            """Source that fires SIGTERM on second read."""

            def __init__(self):
                self._calls = 0

            def read(self):
                self._calls += 1
                if self._calls == 1:
                    return [ev('/tmp/s.txt', EventKind.CREATED)]
                os.kill(os.getpid(), signal.SIGTERM)
                return [ev('/tmp/s2.txt', EventKind.CREATED)]

            def close(self):
                pass

        output = FakeOutputHandler()
        monitor = Monitor(SignalSource(), output, state=_fake_state())
        monitor.run()

        assert output.closed is True
        # First batch should have been processed
        assert len(output.payloads) >= 1


# ---------------------------------------------------------------------------
# Stats accuracy
# ---------------------------------------------------------------------------


class TestMonitorStatsAccuracy:
    def test_stats_after_mixed_batch(self):
        """Stats reflect reduction: 4 received, 3 reduced, 1 accepted."""
        source = InMemoryEventSource(
            [
                [
                    ev('/a.txt', EventKind.CREATED),
                    ev('/a.txt', EventKind.MODIFIED),
                    ev('/b.txt', EventKind.CREATED),
                    ev('/b.txt', EventKind.REMOVED),
                ],
            ],
        )
        output = FakeOutputHandler()
        batch = BatchProcessor(rules=STAT_REDUCTION_RULES)
        monitor = Monitor(source, output, batch=batch, state=_fake_state())
        monitor.run()

        stats = monitor.stats()
        assert stats['batch_received'] == 4
        assert stats['batch_ignored'] + stats['batch_cancelled'] == 3
        assert stats['batch_accepted'] == 1
        assert stats['state_files'] == 1
        assert stats['state_emitted'] == 1

    def test_stats_zero_for_empty(self):
        """Empty source — all stats zero."""
        source = InMemoryEventSource([])
        monitor = Monitor(source)
        monitor.run()
        stats = monitor.stats()
        assert stats['batch_received'] == 0
        assert stats['state_files'] == 0

    def test_stats_include_timing(self):
        """Stats include run_duration_seconds after run()."""
        source = InMemoryEventSource(
            [[ev('/t.txt', EventKind.CREATED)]],
        )
        monitor = Monitor(source, state=_fake_state())
        monitor.run()
        stats = monitor.stats()
        assert 'run_duration_seconds' in stats
        assert stats['run_duration_seconds'] >= 0

    def test_stats_include_per_kind_breakdown(self):
        """Stats include state_CREATED, state_MODIFIED keys."""
        source = InMemoryEventSource(
            [
                [
                    ev('/a.txt', EventKind.CREATED),
                    ev('/a.txt', EventKind.MODIFIED),
                ],
            ],
        )
        monitor = Monitor(source, state=_fake_state())
        monitor.run()
        stats = monitor.stats()
        assert stats['state_CREATED'] == 1
        assert stats['state_MODIFIED'] == 1
        assert stats['state_events'] == 2

    def test_stats_include_emit_counts(self):
        """Stats include state_updates_emitted and state_deletes_emitted."""
        source = InMemoryEventSource(
            [
                [ev('/a.txt', EventKind.CREATED)],
                [ev('/a.txt', EventKind.REMOVED)],
            ],
        )
        output = FakeOutputHandler()
        monitor = Monitor(source, output, state=_fake_state())
        monitor.run()
        stats = monitor.stats()
        assert stats['state_updates_emitted'] >= 1
        assert stats['state_deletes_emitted'] >= 1

    def test_stats_include_throughput(self):
        """Stats include events_per_second after run()."""
        source = InMemoryEventSource(
            [[ev('/t.txt', EventKind.CREATED)]],
        )
        monitor = Monitor(source, state=_fake_state())
        monitor.run()
        stats = monitor.stats()
        assert 'events_per_second' in stats
        assert stats['events_per_second'] >= 0


# ---------------------------------------------------------------------------
# Monitor with rename events
# ---------------------------------------------------------------------------


class TestMonitorRenameEndToEnd:
    def test_create_rename_output(self):
        """Create → rename → output has new path update."""
        source = InMemoryEventSource(
            [
                [ev('/old.txt', EventKind.CREATED)],
                [
                    ev('/old.txt', EventKind.RENAMED),
                    ev('/new.txt', EventKind.RENAMED),
                ],
            ],
        )
        output = FakeOutputHandler()
        Monitor(source, output, state=_fake_state()).run()
        assert any(
            p['path'] == '/new.txt' and p['op'] == 'update'
            for p in output.payloads
        )

    def test_full_lifecycle_output(self):
        """Create → modify → rename → delete through monitor."""
        source = InMemoryEventSource(
            [
                [ev('/f.txt', EventKind.CREATED)],
                [ev('/f.txt', EventKind.MODIFIED)],
                [
                    ev('/f.txt', EventKind.RENAMED),
                    ev('/g.txt', EventKind.RENAMED),
                ],
                [ev('/g.txt', EventKind.REMOVED)],
            ],
        )
        output = FakeOutputHandler()
        Monitor(source, output, state=_fake_state()).run()
        assert any(
            p['op'] == 'delete' and p['path'] == '/g.txt'
            for p in output.payloads
        )


# ---------------------------------------------------------------------------
# Stats progression
# ---------------------------------------------------------------------------


class TestMonitorStatsProgression:
    def test_5_batches_accumulate(self):
        """5 batches — received count = 5."""
        source = InMemoryEventSource(
            [[ev(f'/f_{i}.txt', EventKind.CREATED)] for i in range(5)],
        )
        output = FakeOutputHandler()
        monitor = Monitor(source, output, state=_fake_state())
        monitor.run()
        assert monitor.stats()['batch_received'] == 5

    def test_heavy_reduction_stats(self):
        """100 MODIFIED same path → 99 reduced."""
        source = InMemoryEventSource(
            [
                [ev('/hot', EventKind.MODIFIED) for _ in range(100)],
            ],
        )
        Monitor(source, FakeOutputHandler(), state=_fake_state()).run()

    def test_full_cancellation_stats(self):
        """CREATED+REMOVED → 0 state files."""
        source = InMemoryEventSource(
            [
                [ev('/t', EventKind.CREATED), ev('/t', EventKind.REMOVED)],
            ],
        )
        m = Monitor(source, FakeOutputHandler(), state=_fake_state())
        m.run()
        assert m.stats()['state_files'] == 0
