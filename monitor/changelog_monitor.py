"""Changelog monitor orchestration."""

from __future__ import annotations

import json
import logging
import time
import urllib.request
from datetime import datetime
from enum import StrEnum
from typing import Any

from monitor.batch_processor import GPFSBatchProcessor
from monitor.batch_processor import LFSBatchProcessor
from monitor.changelog_client import GPFSChangelogClient
from monitor.changelog_client import LFSChangelogClient
from monitor.changelog_client import LFSClient
from monitor.conf import Config
from monitor.mpsc_queue import SharedBatchQueue
from monitor.output_handlers import get_output_handler
from monitor.output_handlers import OutputHandler
from monitor.state_manager import GPFSStateManager
from monitor.state_manager import LFSStateManager

Batch = list[dict[str, Any]]
logger = logging.getLogger(__name__)


class GPFSRole(StrEnum):
    """GPFS Monitor roles."""

    MONITOR = 'monitor'  # single-process: changelog + state + output
    CWORKER = 'cworker'  # changelog worker: read -> queue
    UWORKER = 'uworker'  # update worker: queue -> state -> output


class LFSRole(StrEnum):
    """LFS Monitor roles."""

    MONITOR = 'monitor'  # single-process


def _format_json(payload: Any) -> str:
    return json.dumps(payload, indent=2, sort_keys=False)


def _get_public_ip() -> str:  # for EC2
    try:
        return (
            urllib.request.urlopen(
                'http://169.254.169.254/latest/meta-data/public-ipv4',
                timeout=1,
            )
            .read()
            .decode()
        )
    except Exception:
        return 'unknown'


class GPFSChangelogMonitor:
    """Multipurpose GPFS changelog orchestrator.

    role = monitor: single-process monitor, consumes the mmwatch topic
    role = cworker: changelog worker, consumes a partition, sends to channel
    role = uworker: update worker, receives from channel, sends updates
    """

    def __init__(
        self,
        config: Config,
        role: GPFSRole = GPFSRole.MONITOR,
        channel: SharedBatchQueue | None = None,
        *,
        flush_every: int = 1,  # TODO: general config
    ) -> None:
        """Initialize monitor variables."""
        self.logger = logging.getLogger(__name__)
        self.config = config
        self.role = GPFSRole(role)
        self.channel = channel
        self.flush_every = flush_every

        self._run_start_time: float | None = None
        self._run_end_time: float | None = None

        self.client: GPFSChangelogClient | None = None
        self.batch_processor: GPFSBatchProcessor | None = None
        self.state_manager: GPFSStateManager | None = None
        self.output_handler: OutputHandler | None = None

        self._batches_emitted = 0
        self._events_emitted = 0
        self._batches_processed = 0
        self._events_processed = 0
        self._termination_signals_needed: int | None = None

        self._init_components()

    def _init_components(self) -> None:
        if self.role in (GPFSRole.MONITOR, GPFSRole.CWORKER):
            self.client = GPFSChangelogClient(self.config.gpfs)
            self.batch_processor = GPFSBatchProcessor(
                self.config.batch_processor,
            )

        if self.role in (GPFSRole.MONITOR, GPFSRole.UWORKER):
            self.state_manager = GPFSStateManager(
                print_changelogs=self.config.general.print_changelogs,
            )
            self.output_handler = get_output_handler(self.config.output)

        if self.role in (GPFSRole.CWORKER, GPFSRole.UWORKER):
            if self.channel is None:
                raise ValueError('cworker/uworker requires a SharedBatchQueue')

            if self.role == GPFSRole.UWORKER:
                self._termination_signals_needed = self.config.gpfs.partition
                assert self._termination_signals_needed > 0

    def run(self) -> None:
        """Continuously read changelog events until the stream is exhausted."""
        if self.role is GPFSRole.MONITOR:
            self._run_monitor()
        elif self.role is GPFSRole.CWORKER:
            self._run_cworker()
        else:
            self._run_uworker()

        if self.role in (GPFSRole.MONITOR, GPFSRole.UWORKER):
            self._flush_updates()

    def close(self) -> None:
        """Close resources and log run summary details."""
        if self.client is not None:
            self.client.close()
        if self.output_handler is not None:
            self.output_handler.close()

        stats = self._collect_run_stats()
        self.logger.info('Run summary: %s', self._format_run_summary(stats))

    def _run_monitor(self) -> None:
        self._mark_run_start(logging.INFO)
        assert self.client
        assert self.batch_processor

        while True:
            events = self.client.read()
            if not events:
                break
            self.batch_processor.add_events(events)
            batch = self._commit_batch()
            if batch:
                self._handle_state_batch(batch)
        self._mark_run_end(logging.INFO)

    def _run_cworker(self) -> None:
        self._mark_run_start(logging.INFO)
        assert self.client
        assert self.batch_processor
        assert self.channel

        idle_grace = self.config.gpfs.idle_grace_seconds
        idle_deadline = (
            time.monotonic() + idle_grace if idle_grace > 0 else None
        )

        while True:
            events = self.client.read()
            if not events:
                if idle_deadline is None:
                    self.channel.put(None)
                    break

                if time.monotonic() >= idle_deadline:
                    self.logger.info(
                        'cworker idle for %.3fs, stopping',
                        idle_grace,
                    )
                    self.channel.put(None)
                    break
                continue

            idle_deadline = (
                time.monotonic() + idle_grace if idle_grace > 0 else None
            )
            self.batch_processor.add_events(events)
            batch = self._commit_batch()
            if batch:
                try:
                    self.channel.put(batch)
                except ValueError:
                    self.logger.exception(
                        'Increase SharedBatchQueue.max_payload_bytes.',
                    )
                    raise
                self._batches_emitted += 1
                self._events_emitted += len(batch)
        self._mark_run_end(logging.DEBUG)

    def _run_uworker(self) -> None:
        self._mark_run_start(logging.INFO)
        assert self.channel
        assert self._termination_signals_needed is not None

        while self._termination_signals_needed > 0:
            batch = self.channel.get()
            if batch is None:
                self._termination_signals_needed -= 1
                continue
            self._handle_state_batch(batch)
        self._mark_run_end(logging.INFO)

    def _commit_batch(self) -> Batch:
        # shared by monitor and cworker
        assert self.client
        assert self.batch_processor

        self.client.commit()
        return self.batch_processor.commit_batch()

    def _handle_state_batch(self, batch: Batch) -> None:
        # shared by monitor and uworker
        assert self.state_manager
        assert self.output_handler

        self._batches_processed += 1
        self._events_processed += len(batch)

        if self.config.general.changelog_mode:
            self.output_handler.send(batch)
            return

        self.state_manager.process_events(batch)

        # snapshot = self.state_manager.snapshot()
        # formatted_snapshot = _format_json(snapshot)
        # self.logger.info('State snapshot:\n%s', formatted_snapshot)

        if self._batches_processed % self.flush_every == 0:
            updates = self.state_manager.emit_updates()
            if updates:
                self.output_handler.send(updates)
                # formatted_updates = _format_json(updates)
                # self.logger.info('State updates:\n%s', formatted_updates)

    def _flush_updates(self) -> None:
        # shared by monitor and uworker
        if self.config.general.changelog_mode:
            return
        if not (self.state_manager and self.output_handler):
            return
        updates = self.state_manager.emit_updates()
        if updates:
            self.output_handler.send(updates)
            # formatted_updates = _format_json(updates)
            # self.logger.info('State updates:\n%s', formatted_updates)

    def _mark_run_start(self, level: int) -> None:
        self._run_start_time = time.time()
        self.logger.log(
            level,
            'Monitor (%s) started at %s',
            self.role.value,
            datetime.fromtimestamp(self._run_start_time).isoformat(),
        )

    def _mark_run_end(self, level: int) -> None:
        self._run_end_time = time.time()
        self.logger.log(
            level,
            'Monitor (%s) finished at %s',
            self.role.value,
            datetime.fromtimestamp(self._run_end_time).isoformat(),
        )

    def _collect_run_stats(self) -> dict[str, object]:
        start_iso = (
            datetime.fromtimestamp(self._run_start_time).isoformat()
            if self._run_start_time
            else ''
        )
        end_iso = (
            datetime.fromtimestamp(self._run_end_time).isoformat()
            if self._run_end_time
            else ''
        )
        duration = (
            self._run_end_time - self._run_start_time
            if self._run_start_time and self._run_end_time
            else None
        )
        idle_grace = self.config.gpfs.idle_grace_seconds
        duration_adjusted = duration
        if (
            duration is not None
            and idle_grace > 0
            and self.role
            in (
                GPFSRole.CWORKER,
                GPFSRole.UWORKER,
            )
        ):
            # Exclude trailing idle grace from throughput-facing duration.
            duration_adjusted = max(0.0, duration - idle_grace)

        stats: dict[str, object] = {
            'role': self.role.value,
            'partition': self.config.gpfs.partition,
            'run_start_iso': start_iso,
            'run_end_iso': end_iso,
            'run_duration_seconds': duration_adjusted,
            'run_duration_seconds_raw': duration,
            'idle_grace_seconds': idle_grace,
            'batches_emitted': self._batches_emitted,
            'events_emitted': self._events_emitted,
            'batches_processed': self._batches_processed,
            'events_processed': self._events_processed,
        }
        if duration_adjusted and duration_adjusted > 0:
            if self._events_processed > 0:
                stats['events_processed_per_second'] = (
                    self._events_processed / duration_adjusted
                )
            if self._events_emitted > 0:
                stats['events_emitted_per_second'] = (
                    self._events_emitted / duration_adjusted
                )

        if self.client and self.batch_processor:
            stats.update(self.client.stats_summary())
            stats.update(self.batch_processor.stats_summary())
        if self.state_manager:
            stats.update(self.state_manager.stats_summary())
        return stats

    @staticmethod
    def _format_run_summary(stats: dict[str, object]) -> str:
        return ', '.join(f'{k}, {v}' for k, v in stats.items())


class LFSChangelogMonitor:
    """Multipurpose Lustre changelog orchestrator."""

    def __init__(
        self,
        config: Config,
        role: LFSRole = LFSRole.MONITOR,
        channel: SharedBatchQueue | None = None,
        *,
        flush_every: int = 1,  # TODO: general config
    ) -> None:
        """Initialize monitor variables."""
        self.logger = logging.getLogger(__name__)
        self.config = config
        self.role = LFSRole(role)
        self.channel = channel
        self.flush_every = flush_every

        self._run_start_time: float | None = None
        self._run_end_time: float | None = None

        self.client: LFSChangelogClient | None = None
        self.batch_processor: LFSBatchProcessor | None = None
        self.state_manager: LFSStateManager | None = None
        self.output_handler: OutputHandler | None = None

        self._batches_emitted = 0
        self._events_emitted = 0
        self._batches_processed = 0
        self._events_processed = 0
        self._termination_signals_needed: int | None = None

        self._init_components()

    def _init_components(self) -> None:
        assert self.role == LFSRole.MONITOR

        self.client = LFSChangelogClient(self.config.lustre)
        self.batch_processor = LFSBatchProcessor(self.config.batch_processor)
        lfs_client = LFSClient(
            self.config.lustre.mdt,
            self.config.lustre.cid,
            self.config.lustre.fsname,
            self.config.lustre.fs_mount_point,
        )
        self.state_manager = LFSStateManager(
            self.config.lustre.fs_mount_point,
            lfs_client,
            self.config.general.print_changelogs,
            self.config.lustre.fid_resolution_method,
        )
        self.output_handler = get_output_handler(self.config.output)

    def run(self) -> None:
        """Continuously read Lustre changelog events."""
        assert self.role == LFSRole.MONITOR
        self._run_monitor()

    def close(self) -> dict[str, object]:
        """Close resources and log run summary details."""
        if self.client is not None:
            self.client.close()

        if self.output_handler is not None:
            self.output_handler.close()
        stats = self._collect_run_stats()
        self.logger.info('Run summary: %s', self._format_run_summary(stats))
        return stats

    def _run_monitor(self) -> None:
        """Canonical single-process Lustre monitor loop."""
        self._mark_run_start(logging.INFO)
        assert self.client
        assert self.batch_processor

        start_time = time.time()
        stop_after = 120  # seconds

        while True:
            events = self.client.read()
            if not events:
                break
            self.batch_processor.add_events(events)
            batch = self._commit_batch()
            if batch:
                self._handle_state_batch(batch)

            # exit condition when using FSMONITOR resolution
            if (
                # self.config.lustre.fid_resolution_method
                # == LustreFidResolutionMethod.FSMONITOR
                # and
                (time.time() - start_time) > stop_after
            ):  # TODOYY
                self.logger.info(
                    f'timeout reached: {stop_after}s — stopping.',
                )
                break

        self._flush_updates()
        self._mark_run_end(logging.INFO)

    def _commit_batch(self) -> Batch:
        """Flush accumulated Lustre events to a batch."""
        assert self.client
        assert self.batch_processor

        # self.client.commit() # to bmk
        return self.batch_processor.commit_batch()

    def _handle_state_batch(self, batch: Batch) -> None:
        """Apply a committed batch to the state/output pipeline."""
        assert self.state_manager
        assert self.output_handler

        self._batches_processed += 1
        self._events_processed += len(batch)

        if self.config.general.changelog_mode:
            self.output_handler.send(batch)
            return

        self.state_manager.process_events(batch)

        if self._batches_processed % self.flush_every == 0:
            updates = self.state_manager.emit_updates()
            if updates:
                self.output_handler.send(updates)

    def _flush_updates(self) -> None:
        """Send any pending metadata updates downstream."""
        if self.config.general.changelog_mode:
            return
        if not (self.state_manager and self.output_handler):
            return
        updates = self.state_manager.emit_updates()
        if updates:
            self.output_handler.send(updates)

    def _mark_run_start(self, level: int) -> None:
        self._run_start_time = time.time()
        self.logger.log(
            level,
            'Lustre monitor started at %s',
            datetime.fromtimestamp(self._run_start_time).isoformat(),
        )

    def _mark_run_end(self, level: int) -> None:
        self._run_end_time = time.time()
        self.logger.log(
            level,
            'Lustre monitor finished at %s',
            datetime.fromtimestamp(self._run_end_time).isoformat(),
        )

    def _collect_run_stats(self) -> dict[str, object]:
        start_iso = (
            datetime.fromtimestamp(self._run_start_time).isoformat()
            if self._run_start_time
            else ''
        )
        end_iso = (
            datetime.fromtimestamp(self._run_end_time).isoformat()
            if self._run_end_time
            else ''
        )
        duration = (
            self._run_end_time - self._run_start_time
            if self._run_start_time and self._run_end_time
            else None
        )
        stats: dict[str, object] = {
            'public_ip': _get_public_ip(),
            'role': self.role.value,
            'mdt': self.config.lustre.mdt,
            'cid': self.config.lustre.cid,
            'fid': self.config.lustre.fid_resolution_method,
            'run_start_iso': start_iso,
            'run_end_iso': end_iso,
            'run_duration_seconds': duration,
            'batches_processed': self._batches_processed,
            'events_processed': self._events_processed,
        }
        if self.client:
            stats.update(self.client.stats_summary())
        if self.batch_processor:
            stats.update(self.batch_processor.stats_summary())
        if self.state_manager:
            stats.update(self.state_manager.stats_summary())
        if self.state_manager and self.state_manager.lfs_client:
            stats['fid2path_attempts'] = (
                self.state_manager.lfs_client.fid2path_attempts
            )
        return stats

    @staticmethod
    def _format_run_summary(stats: dict[str, object]) -> str:
        return ', '.join(f'{k}, {v}' for k, v in stats.items())
