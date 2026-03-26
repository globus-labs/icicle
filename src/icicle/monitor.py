"""Top-level monitor orchestrator for icicle."""

from __future__ import annotations

import logging
import os
import signal
import time
from typing import Any

from src.icicle.batch import BatchProcessor
from src.icicle.output import OutputHandler
from src.icicle.source import EventSource
from src.icicle.state import BaseStateManager
from src.icicle.state import PathStateManager

logger = logging.getLogger(__name__)


class Monitor:
    """Wires EventSource -> BatchProcessor -> StateManager -> OutputHandler."""

    def __init__(
        self,
        source: EventSource,
        output: OutputHandler | None = None,
        batch: BatchProcessor | None = None,
        state: BaseStateManager | None = None,
        changelog_mode: bool = False,
    ) -> None:
        """Initialize monitor with source and output.

        Args:
            source: Event source to read from.
            output: Output handler. Defaults to None (no output).
            batch: BatchProcessor. Defaults to no-rules BatchProcessor.
            state: State manager. Defaults to PathStateManager.
            changelog_mode: If True, send raw events directly to output
                bypassing state processing.
        """
        self.source = source
        self.output = output
        self.batch = batch if batch is not None else BatchProcessor()
        self.changelog_mode = changelog_mode
        self.state: BaseStateManager | None = (
            None
            if changelog_mode
            else (state if state is not None else PathStateManager())
        )
        self._running = False
        self._start: float | None = None
        self._end: float | None = None
        self._start_ts: float | None = None
        self._end_ts: float | None = None

    def run(self) -> None:
        """Main event loop. Exits when source returns empty or on signal."""
        self._running = True

        def _shutdown(signum: int, frame: Any) -> None:
            if not self._running:
                logger.info('Forced exit.')
                os._exit(1)
            logger.info('Signal %d received, shutting down.', signum)
            self._running = False
            self.source.close()

        signal.signal(signal.SIGINT, _shutdown)
        signal.signal(signal.SIGTERM, _shutdown)

        self._start = time.monotonic()
        self._start_ts = time.time()
        try:
            while self._running:
                events = self.source.read()
                if not events:
                    break

                self.batch.add_events(events)
                coalesced = self.batch.flush()
                if coalesced:
                    self._process_coalesced(coalesced)
        finally:
            self._end = time.monotonic()
            self._end_ts = time.time()
            self._finalize()

    def _process_coalesced(self, coalesced: list[dict[str, Any]]) -> None:
        """Route coalesced events to changelog output or state manager."""
        if self.changelog_mode:
            if self.output is not None:
                self.output.send(coalesced)
        else:
            if self.state is None:
                raise RuntimeError(
                    'state manager is None in stateful mode',
                )
            self.state.process_events(coalesced)
            self._flush()

    def _finalize(self) -> None:
        """Finalize pipeline after the event loop exits."""
        if not self.changelog_mode:
            state = self.state
            if state is not None and hasattr(state, 'close_pending'):
                state.close_pending()
            self._flush()
        if self.output is not None:
            self.output.flush()
        self.close()

    def _flush(self) -> None:
        if self.state is None:
            return
        updates = self.state.emit()
        if updates and self.output is not None:
            self.output.send(updates)

    def close(self) -> None:
        """Close source and output."""
        self.source.close()
        if self.output is not None:
            self.output.close()

    def stats(self) -> dict[str, Any]:
        """Return combined stats from all pipeline components."""
        result: dict[str, Any] = {}

        # Timing.
        duration: float | None = None
        if self._start_ts is not None:
            result['run_start'] = round(self._start_ts, 3)
        if self._end_ts is not None:
            result['run_end'] = round(self._end_ts, 3)
        if self._start is not None and self._end is not None:
            duration = self._end - self._start
            result['run_duration_seconds'] = round(duration, 2)

        # Source stats.
        result.update(self.source.stats())

        # Batch stats.
        result['batch_received'] = self.batch.received
        result['batch_ignored'] = self.batch.ignored
        result['batch_cancelled'] = self.batch.cancelled
        result['batch_accepted'] = self.batch.accepted

        # State stats.
        if self.state is not None:
            result.update(self.state.stats())

        # Throughput: total changelog records (including source-ignored).
        if duration and duration > 0:
            source_total = result.get('source_events', 0) + result.get(
                'source_ignored',
                0,
            )
            result['events_per_second'] = round(
                source_total / duration,
                1,
            )

        return result
