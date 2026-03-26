"""Lustre changelog event source for icicle."""

from __future__ import annotations

import contextlib
import logging
import subprocess
import time
from typing import Any

from src.icicle.events import EventKind
from src.icicle.lustre_events import LUSTRE_EVENT_MAP
from src.icicle.lustre_events import LustreEventType
from src.icicle.lustre_events import parse_changelog_line
from src.icicle.source import EventSource

logger = logging.getLogger(__name__)


class LustreChangelogSource(EventSource):
    """Reads events from Lustre changelogs via ``lfs changelog``."""

    def __init__(  # noqa: PLR0913
        self,
        mdt: str,
        mount_point: str,
        fsname: str,
        poll_interval: float = 1.0,
        startrec: int = 0,
        fid_resolution_method: str = 'icicle',
        ignore_events: frozenset[LustreEventType] | None = None,
        max_batch_size: int = 100_000,
        drain: bool = False,
    ) -> None:
        """Initialize the Lustre changelog source.

        Args:
            mdt: MDT name (e.g. 'fs0-MDT0000').
            mount_point: Filesystem mount point (e.g. '/mnt/fs0').
            fsname: Lustre filesystem name (e.g. 'fs0').
            poll_interval: Seconds to sleep when no new records.
            startrec: First changelog record number to read from.
            fid_resolution_method: How to resolve FIDs to paths.
                'naive' — always call fid2path (no cache).
                'fsmonitor' — cache + fid2path fallback.
                'icicle' — in-memory path construction from parent + name.
            ignore_events: Lustre event types to skip at source level.
                None means no source-level filtering (events not in
                LUSTRE_EVENT_MAP are still dropped by _resolve_event).
            max_batch_size: Maximum changelog records per read cycle.
            drain: If True, return [] on first empty poll instead of
                waiting for new records. Use for benchmarking.
        """
        self._mdt = mdt
        self._mount_point = mount_point.rstrip('/')
        self._fsname = fsname
        self._poll_interval = poll_interval
        self._startrec = startrec
        self._closed = False
        self._ignore_events = ignore_events
        self._max_batch_size = max_batch_size
        self._drain = drain
        self._uses_cache = fid_resolution_method != 'naive'
        self._is_icicle = fid_resolution_method == 'icicle'

        # Counters for stats().
        self._polls = 0
        self._events = 0
        self._ignored = 0
        self._errors = 0
        self._fid2path_calls = 0
        self._fid2path_time = 0.0

        # FID-to-path cache (used by icicle/fsmonitor; empty for naive).
        self._fid_to_path: dict[str, str] = {}
        self._fid_is_dir: dict[str, bool] = {}
        self._proc: subprocess.Popen[str] | None = None

        # Bind FID resolver based on strategy.
        self._resolve_fid = (
            self._resolve_fid_cached
            if self._uses_cache
            else self._resolve_fid_naive
        )

        logger.info(
            'LustreChangelogSource: mdt=%s mount=%s fsname=%s '
            'startrec=%d fid_resolution=%s',
            mdt,
            mount_point,
            fsname,
            startrec,
            fid_resolution_method,
        )

    def read(self) -> list[dict[str, Any]]:
        """Read new changelog records and return resolved events.

        Blocks (polling) until at least one event is available.
        Returns events with ``kind=EventKind`` (mapped from LustreEventType).
        Returns empty list only when closed.
        """
        while not self._closed:
            self._polls += 1
            lines = self._read_changelog()
            if not lines:
                if self._drain:
                    return []
                time.sleep(self._poll_interval)
                continue

            events: list[dict[str, Any]] = []
            last_eid: int | None = None

            for line in lines:
                parsed = parse_changelog_line(line)
                if parsed is None:
                    continue

                last_eid = parsed['eid']
                if (
                    self._ignore_events
                    and parsed['lustre_type'] in self._ignore_events
                ):
                    self._ignored += 1
                    continue
                resolved = self._resolve_event(parsed)
                if not resolved:
                    self._ignored += 1
                else:
                    events.extend(resolved)

            if last_eid is not None:
                self._startrec = last_eid + 1

            if events:
                self._events += len(events)
                return events

        return []

    def close(self) -> None:
        """Mark the source as closed and kill any running subprocess."""
        self._closed = True
        if self._proc is not None:
            with contextlib.suppress(OSError):
                self._proc.kill()
        logger.info('LustreChangelogSource closed.')

    def stats(self) -> dict[str, Any]:
        """Return source-level counters."""
        return {
            'source_polls': self._polls,
            'source_events': self._events,
            'source_ignored': self._ignored,
            'source_errors': self._errors,
            'source_fid2path_calls': self._fid2path_calls,
            'source_fid2path_time': self._fid2path_time,
        }

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _read_changelog(self) -> list[str]:
        """Run ``lfs changelog | head`` and return up to max_batch_size lines.

        Pipes through ``head -n`` so the kernel sends SIGPIPE to
        ``lfs changelog`` as soon as enough lines are read, avoiding
        the cost of buffering millions of unneeded records.
        """
        cmd = (
            f'sudo lfs changelog {self._mdt} {self._startrec}'
            f' | head -n {self._max_batch_size}'
        )
        try:
            self._proc = subprocess.Popen(
                cmd,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            output, _ = self._proc.communicate()
            self._proc = None
            if not output:
                return []
            return output.splitlines()
        except OSError as exc:
            logger.error('lfs changelog failed: %s', exc)
            self._proc = None
            self._errors += 1
            return []

    def _resolve_event(
        self,
        parsed: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Resolve a parsed changelog dict into event dicts with paths.

        Returns 0, 1, or 2 events (RENME produces a pair).
        """
        lustre_type: LustreEventType = parsed['lustre_type']
        kind = LUSTRE_EVENT_MAP.get(lustre_type)
        if kind is None:
            return []

        ts = parsed['ts']
        target_fid = parsed['target_fid']

        if lustre_type in (LustreEventType.CREAT, LustreEventType.MKDIR):
            return self._resolve_create(parsed, kind, ts)

        if lustre_type in (LustreEventType.UNLNK, LustreEventType.RMDIR):
            return self._resolve_delete(parsed, kind, ts)

        if lustre_type == LustreEventType.RENME:
            return self._resolve_rename(parsed, ts)

        # CLOSE, TRUNC, SATTR, MTIME, CTIME, ATIME — metadata/data change.
        path = self._resolve_fid(target_fid)
        if path is None:
            return []
        return [{'path': path, 'kind': kind, 'ts': ts, 'is_dir': False}]

    def _resolve_create(
        self,
        parsed: dict[str, Any],
        kind: EventKind,
        ts: str,
    ) -> list[dict[str, Any]]:
        """Resolve CREAT/MKDIR: build path from parent + name, update cache."""
        parent_fid = parsed['parent_fid']
        name = parsed['name']
        target_fid = parsed['target_fid']
        is_dir = parsed['lustre_type'] == LustreEventType.MKDIR

        if parent_fid is None or name is None:
            return []

        path: str
        if self._is_icicle:
            parent_path = self._resolve_fid(parent_fid)
            if parent_path is None:
                return []
            path = f'{parent_path}/{name}'
        else:  # naive, fsmonitor — resolve target directly
            parent_path = self._resolve_fid(parent_fid)
            resolved = self._fid2path(target_fid) if target_fid else None
            if resolved is None and parent_path is not None and name:
                resolved = f'{parent_path}/{name}'
            if resolved is None:
                return []
            path = resolved

        if target_fid and self._uses_cache:
            self._fid_to_path[target_fid] = path
            self._fid_is_dir[target_fid] = is_dir

        return [{'path': path, 'kind': kind, 'ts': ts, 'is_dir': is_dir}]

    def _resolve_delete(
        self,
        parsed: dict[str, Any],
        kind: EventKind,
        ts: str,
    ) -> list[dict[str, Any]]:
        """Resolve UNLNK/RMDIR: look up path, remove from cache."""
        target_fid = parsed['target_fid']
        is_dir = parsed['lustre_type'] == LustreEventType.RMDIR

        if self._uses_cache:
            path = self._fid_to_path.get(target_fid) if target_fid else None
        else:
            path = self._fid2path(target_fid) if target_fid else None

        if path is None:
            parent_fid = parsed['parent_fid']
            name = parsed['name']
            if parent_fid and name:
                parent_path = self._resolve_fid(parent_fid)
                if parent_path:
                    path = f'{parent_path}/{name}'

        if path is None:
            return []

        if target_fid and self._uses_cache:
            self._fid_to_path.pop(target_fid, None)
            self._fid_is_dir.pop(target_fid, None)

        return [{'path': path, 'kind': kind, 'ts': ts, 'is_dir': is_dir}]

    def _resolve_rename(
        self,
        parsed: dict[str, Any],
        ts: str,
    ) -> list[dict[str, Any]]:
        """Resolve RENME: emit TWO RENAMED events (old, new) for pairing."""
        source_fid = parsed['source_fid']
        source_parent_fid = parsed['source_parent_fid']
        old_name = parsed['old_name']
        new_parent_fid = parsed['parent_fid']
        new_name = parsed['name']

        if not all(
            [
                source_fid,
                source_parent_fid,
                old_name,
                new_parent_fid,
                new_name,
            ],
        ):
            return []

        if self._uses_cache:
            return self._resolve_rename_cached(
                source_fid, source_parent_fid,
                old_name, new_parent_fid, new_name, ts,
            )
        return self._resolve_rename_naive(
            source_fid, source_parent_fid,
            old_name, new_parent_fid, new_name, ts,
        )

    def _resolve_rename_naive(  # noqa: PLR0913
        self,
        source_fid: str,
        source_parent_fid: str,
        old_name: str,
        new_parent_fid: str,
        new_name: str,
        ts: str,
    ) -> list[dict[str, Any]]:
        """Resolve rename via fid2path only (no cache)."""
        old_path = self._fid2path(source_fid)
        if old_path is None:
            old_parent_path = self._fid2path(source_parent_fid)
            if old_parent_path is None:
                return []
            old_path = f'{old_parent_path}/{old_name}'
        new_parent_path = self._fid2path(new_parent_fid)
        if new_parent_path is None:
            return []
        new_path = f'{new_parent_path}/{new_name}'
        return self._rename_event_pair(
            old_path, new_path, ts, is_dir=False,
        )

    def _resolve_rename_cached(  # noqa: PLR0913
        self,
        source_fid: str,
        source_parent_fid: str,
        old_name: str,
        new_parent_fid: str,
        new_name: str,
        ts: str,
    ) -> list[dict[str, Any]]:
        """Resolve rename using cache, with fid2path fallback."""
        old_path = self._fid_to_path.get(source_fid)
        if old_path is None:
            old_parent_path = self._resolve_fid(source_parent_fid)
            if old_parent_path is None:
                return []
            old_path = f'{old_parent_path}/{old_name}'

        new_parent_path = self._resolve_fid(new_parent_fid)
        if new_parent_path is None:
            return []
        new_path = f'{new_parent_path}/{new_name}'

        is_dir = self._fid_is_dir.get(source_fid, False)

        # Update cache for the renamed entry.
        if source_fid:
            self._fid_to_path[source_fid] = new_path

        # Update cached child paths under renamed directories.
        if is_dir:
            self._update_cached_children(old_path, new_path)

        return self._rename_event_pair(old_path, new_path, ts, is_dir)

    @staticmethod
    def _rename_event_pair(
        old_path: str,
        new_path: str,
        ts: str,
        is_dir: bool,
    ) -> list[dict[str, Any]]:
        """Build the two RENAMED events for rename pairing."""
        return [
            {
                'path': old_path,
                'kind': EventKind.RENAMED,
                'ts': ts,
                'is_dir': is_dir,
            },
            {
                'path': new_path,
                'kind': EventKind.RENAMED,
                'ts': ts,
                'is_dir': is_dir,
            },
        ]

    def _update_cached_children(
        self, old_path: str, new_path: str,
    ) -> None:
        """Update all cached child paths under a renamed directory."""
        old_prefix = old_path + '/'
        for fid, cached_path in list(self._fid_to_path.items()):
            if cached_path.startswith(old_prefix):
                self._fid_to_path[fid] = (
                    new_path + cached_path[len(old_path):]
                )

    def _resolve_fid_naive(self, fid: str | None) -> str | None:
        """Resolve a FID via fid2path (no cache)."""
        if fid is None:
            return None
        return self._fid2path(fid)

    def _resolve_fid_cached(self, fid: str | None) -> str | None:
        """Resolve a FID using cache, with fid2path fallback."""
        if fid is None:
            return None
        cached = self._fid_to_path.get(fid)
        if cached is not None:
            return cached
        path = self._fid2path(fid)
        if path is not None:
            self._fid_to_path[fid] = path
        return path

    def _fid2path(self, fid: str) -> str | None:
        """Resolve a FID via ``lfs fid2path`` subprocess (slow fallback)."""
        self._fid2path_calls += 1
        logger.debug('fid2path fallback for FID %s', fid)
        t0 = time.monotonic()
        try:
            result = subprocess.run(
                ['sudo', 'lfs', 'fid2path', self._fsname, fid],
                capture_output=True,
                text=True,
                check=True,
            )
            relative = result.stdout.strip()
            if not relative:
                return None
            if relative == '/':
                return self._mount_point
            return f'{self._mount_point}/{relative}'
        except subprocess.CalledProcessError:
            return None
        finally:
            self._fid2path_time += time.monotonic() - t0
