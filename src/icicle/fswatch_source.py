"""Fswatch event source for icicle."""

from __future__ import annotations

import logging
import platform
import select
import subprocess
from typing import Any

from src.icicle.fswatch_events import FLAG_SEPARATOR
from src.icicle.fswatch_events import FSWATCH_EVENTS
from src.icicle.fswatch_events import parse_fswatch_line
from src.icicle.source import EventSource

logger = logging.getLogger(__name__)

# Type indicator flags — never used for ignore filtering.
_TYPE_FLAGS = frozenset({'IsFile', 'IsDir', 'IsSymLink'})


class FSWatchSource(EventSource):
    """Reads events from an fswatch subprocess."""

    def __init__(  # noqa: PLR0913
        self,
        watch_dir: str,
        latency: float = 0.1,
        ignore_flags: frozenset[str] | None = None,
        max_batch_size: int = 10_000,
        drain: bool = False,
        drain_timeout: float = 2.0,
    ) -> None:
        """Launch fswatch as a subprocess.

        Args:
            watch_dir: Directory to watch.
            latency: fswatch latency in seconds.
            ignore_flags: Raw fswatch flag names to filter (e.g. 'Updated').
                Type indicator flags (IsFile/IsDir/IsSymLink) are excluded
                from matching. None means no filtering.
            max_batch_size: Maximum events returned per read() call.
            drain: If True, return [] when no events arrive within
                drain_timeout instead of blocking forever.
            drain_timeout: Seconds to wait before returning [] in drain mode.
        """
        self._ignore_flags = ignore_flags
        self._max_batch_size = max_batch_size
        self._drain = drain
        self._drain_timeout = drain_timeout

        # Counters for stats().
        self._polls = 0
        self._events = 0
        self._ignored = 0
        cmd = [
            'fswatch',
            '-r',
            '-x',
            '-t',
            '-f',
            '%s',
            '--event-flag-separator',
            FLAG_SEPARATOR,
            '-l',
            str(latency),
        ]
        # --no-defer is only supported on macOS (FSEvents monitor).
        if platform.system() == 'Darwin':
            cmd.append('--no-defer')
        for ev_type in FSWATCH_EVENTS:
            cmd.extend(['--event', ev_type])
        cmd.append(watch_dir)

        self._proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        logger.info(
            'FSWatch started: %s (pid %d)',
            ' '.join(cmd),
            self._proc.pid,
        )

    def read(self) -> list[dict[str, Any]]:
        """Block until at least one meaningful event arrives, then drain.

        Returns empty list only when the fswatch process has terminated
        (i.e. no more events will ever come).
        """
        events: list[dict[str, Any]] = []

        while not events:
            self._polls += 1
            if self._proc.stdout is None or self._proc.poll() is not None:
                return []

            if self._drain:
                ready, _, _ = select.select(
                    [self._proc.stdout],
                    [],
                    [],
                    self._drain_timeout,
                )
                if not ready:
                    return []

            line = self._proc.stdout.readline()
            if not line:
                return []
            self._collect_event(line, events)
            self._drain_pending(events)

        self._events += len(events)
        return events

    def _collect_event(
        self,
        line: str,
        events: list[dict[str, Any]],
    ) -> None:
        """Parse a single line and append to events if not ignored."""
        ev = parse_fswatch_line(line)
        if ev is not None:
            if self._should_ignore(ev):
                self._ignored += 1
            else:
                events.append(ev)

    def _drain_pending(self, events: list[dict[str, Any]]) -> None:
        """Drain any additional lines available right now."""
        assert self._proc.stdout is not None
        while len(events) < self._max_batch_size:
            ready, _, _ = select.select(
                [self._proc.stdout],
                [],
                [],
                0.05,
            )
            if not ready:
                break
            line = self._proc.stdout.readline()
            if not line:
                break
            self._collect_event(line, events)

    def _should_ignore(self, event: dict[str, Any]) -> bool:
        """Return True if the event should be dropped by ignore_flags."""
        if not self._ignore_flags:
            return False
        primary_flags = set(event.get('raw_flags', ())) - _TYPE_FLAGS
        return bool(primary_flags & self._ignore_flags)

    def stats(self) -> dict[str, Any]:
        """Return source-level counters."""
        return {
            'source_polls': self._polls,
            'source_events': self._events,
            'source_ignored': self._ignored,
        }

    def close(self) -> None:
        """Terminate the fswatch subprocess (idempotent)."""
        if self._proc.poll() is None:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._proc.kill()
                self._proc.wait()
            logger.info('FSWatch stopped (pid %d)', self._proc.pid)
