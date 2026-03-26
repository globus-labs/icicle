"""Unit tests for icicle FSWatchSource (subprocess management)."""

from __future__ import annotations

import subprocess
from unittest.mock import MagicMock
from unittest.mock import patch

from src.icicle.fswatch_source import FSWatchSource

# ---------------------------------------------------------------------------
# Close behavior
# ---------------------------------------------------------------------------


class TestFSWatchSourceClose:
    @patch('src.icicle.fswatch_source.subprocess.Popen')
    def test_close_terminates_running_process(self, mock_popen):
        proc = MagicMock()
        proc.poll.return_value = None  # process is running
        proc.pid = 1234
        mock_popen.return_value = proc

        source = FSWatchSource('/tmp/test')
        source.close()

        proc.terminate.assert_called_once()
        proc.wait.assert_called_once_with(timeout=5)
        proc.kill.assert_not_called()

    @patch('src.icicle.fswatch_source.subprocess.Popen')
    def test_close_idempotent_on_dead_process(self, mock_popen):
        proc = MagicMock()
        proc.poll.return_value = 0  # process already dead
        proc.pid = 1234
        mock_popen.return_value = proc

        source = FSWatchSource('/tmp/test')
        source.close()

        proc.terminate.assert_not_called()
        proc.kill.assert_not_called()

    @patch('src.icicle.fswatch_source.subprocess.Popen')
    def test_close_escalates_to_sigkill(self, mock_popen):
        proc = MagicMock()
        proc.poll.return_value = None  # process is running
        proc.pid = 1234
        proc.wait.side_effect = [
            subprocess.TimeoutExpired(cmd='fswatch', timeout=5),
            None,  # succeeds after kill
        ]
        mock_popen.return_value = proc

        source = FSWatchSource('/tmp/test')
        source.close()

        proc.terminate.assert_called_once()
        proc.kill.assert_called_once()
        assert proc.wait.call_count == 2


# ---------------------------------------------------------------------------
# Read behavior
# ---------------------------------------------------------------------------


class TestFSWatchSourceRead:
    @patch('src.icicle.fswatch_source.subprocess.Popen')
    def test_read_returns_empty_when_process_dead(self, mock_popen):
        proc = MagicMock()
        proc.poll.return_value = 1  # process terminated with error
        proc.stdout = MagicMock()
        proc.pid = 1234
        mock_popen.return_value = proc

        source = FSWatchSource('/tmp/test')
        result = source.read()

        assert result == []


# ---------------------------------------------------------------------------
# Platform-specific flags
# ---------------------------------------------------------------------------


class TestFSWatchSourcePlatform:
    @patch('src.icicle.fswatch_source.subprocess.Popen')
    @patch('src.icicle.fswatch_source.platform.system', return_value='Darwin')
    def test_no_defer_on_darwin(self, mock_system, mock_popen):
        proc = MagicMock()
        proc.pid = 1234
        mock_popen.return_value = proc

        FSWatchSource('/tmp/test')

        cmd = mock_popen.call_args[0][0]
        assert '--no-defer' in cmd

    @patch('src.icicle.fswatch_source.subprocess.Popen')
    @patch('src.icicle.fswatch_source.platform.system', return_value='Linux')
    def test_no_defer_absent_on_linux(self, mock_system, mock_popen):
        proc = MagicMock()
        proc.pid = 1234
        mock_popen.return_value = proc

        FSWatchSource('/tmp/test')

        cmd = mock_popen.call_args[0][0]
        assert '--no-defer' not in cmd


# ---------------------------------------------------------------------------
# InMemoryEventSource edge cases
# ---------------------------------------------------------------------------


class TestInMemoryEventSourceEdgeCases:
    def test_empty_batch_signals_eof(self):
        """Empty list from source signals EOF — read returns []."""
        from tests.icicle.conftest import InMemoryEventSource

        source = InMemoryEventSource([[]])
        assert source.read() == []

    def test_close_clears_remaining(self):
        """close() clears remaining batches."""
        from tests.icicle.conftest import InMemoryEventSource

        source = InMemoryEventSource(
            [
                [{'path': '/a', 'kind': 'CREATED', 'ts': 0.0}],
                [{'path': '/b', 'kind': 'CREATED', 'ts': 0.0}],
            ],
        )
        source.close()
        assert source.read() == []

    def test_read_exhausted_returns_empty(self):
        """After all batches consumed, read returns []."""
        from tests.icicle.conftest import InMemoryEventSource

        source = InMemoryEventSource(
            [
                [{'path': '/a', 'kind': 'CREATED', 'ts': 0.0}],
            ],
        )
        source.read()
        assert source.read() == []
