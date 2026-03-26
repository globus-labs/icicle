"""In-memory filesystem state with recursive rename support."""

from __future__ import annotations

import dataclasses
import logging
import os
import time
from abc import ABC
from abc import abstractmethod
from collections import Counter
from collections import defaultdict
from collections.abc import Callable
from typing import Any

from src.icicle.events import EventKind

logger = logging.getLogger(__name__)


class BaseStateManager(ABC):
    """Abstract base for in-memory filesystem state managers."""

    files: dict[str, dict[str, Any]]
    children: dict[str, set[str]]
    to_update: set[str]
    to_delete: set[str]
    emitted: set[str]

    @abstractmethod
    def process_events(self, events: list[dict[str, Any]]) -> None:
        """Update in-memory state from a list of events."""

    @abstractmethod
    def emit(self) -> list[dict[str, Any]]:
        """Return accumulated changes and clear pending state."""

    def stats(self) -> dict[str, Any]:
        """Return state metrics. Override in subclasses."""
        return {}


@dataclasses.dataclass
class _RenamePairState:
    """Tracks the state machine for pairing consecutive RENAMED events.

    On macOS, ``mv A B`` (where B exists) emits three Renamed events:
      1. RENAMED A  (old path)       — paired with →
      2. RENAMED B  (new path)       — completes the pair
      3. RENAMED B  (displaced file) — unpaired, must be absorbed

    ``last_target`` records the destination of the last completed pair
    so that unpaired displacement events can be recognised and skipped
    rather than incorrectly flushed as removals.
    """

    pending: dict[str, Any] | None = None
    age: int = 0
    last_target: str | None = None

    def clear(self) -> None:
        """Reset pending state without clearing last_target."""
        self.pending = None
        self.age = 0


class PathStateManager(BaseStateManager):
    """Maintains a path-keyed in-memory view of the filesystem.

    Handles creation, deletion, updates, and directory renames with
    recursive child path updates — no filesystem rescan needed.
    """

    def __init__(
        self,
        stat_fn: Callable[[str], os.stat_result] = os.stat,
    ) -> None:
        """Initialize empty filesystem state.

        Args:
            stat_fn: Callable to stat a path. Defaults to os.stat.
                     Inject a fake for testing.
        """
        # path -> entry dict {path, name, parent, is_dir, is_symlink, ts}
        self.files: dict[str, dict[str, Any]] = {}
        # parent_path -> set of child paths
        self.children: dict[str, set[str]] = defaultdict(set)

        self.to_update: set[str] = set()
        self.to_delete: set[str] = set()
        self.emitted: set[str] = set()

        self._stat_fn = stat_fn

        # Counters for stats().
        self._event_counts: Counter[str] = Counter()
        self._events_processed = 0
        self._updates_emitted = 0
        self._deletes_emitted = 0
        self._stat_calls = 0
        self._stat_time = 0.0

        self._rename = _RenamePairState()

    def process_events(self, events: list[dict[str, Any]]) -> None:
        """Dispatch events to handlers, updating in-memory state."""
        for event in events:
            kind = event['kind']
            self._events_processed += 1
            self._event_counts[kind.name] += 1
            if kind == EventKind.RENAMED:
                self._handle_renamed(event)
            else:
                # If we have a pending rename and get a non-rename event,
                # treat the pending rename as a removal (renamed out of scope).
                self._flush_unpaired_rename()
                if kind == EventKind.CREATED:
                    self._handle_created(event)
                elif kind == EventKind.REMOVED:
                    self._handle_removed(event)
                else:
                    self._handle_updated(event)

    def emit(self) -> list[dict[str, Any]]:
        """Return accumulated changes and clear pending state.

        A pending rename gets one grace emit() cycle so that a rename
        pair split across two read() batches can be paired.  On the
        second emit() the pending rename is flushed as a removal.
        """
        if self._rename.pending is not None:
            self._rename.age += 1
            if self._rename.age > 1:
                self._flush_unpaired_rename()
        changes: list[dict[str, Any]] = []
        pending_deletes = self.to_delete

        for path in self.to_update - pending_deletes:
            entry = self.files.get(path)
            if not entry:
                continue
            stat = self._stat_path(path)
            if stat is None:
                continue  # File gone before we could stat it.
            changes.append(
                {
                    'op': 'update',
                    'fid': path,
                    'path': path,
                    'stat': stat,
                },
            )
            self.emitted.add(path)

        for path in pending_deletes:
            if path in self.emitted:
                changes.append(
                    {'op': 'delete', 'fid': path, 'path': path},
                )
                self.emitted.discard(path)

        self.to_update.clear()
        self.to_delete.clear()
        for c in changes:
            if c['op'] == 'update':
                self._updates_emitted += 1
            else:
                self._deletes_emitted += 1
        return changes

    def close_pending(self) -> None:
        """Flush any pending rename as removal.

        Call once at shutdown so that a rename-out-of-scope that was
        still waiting for its pair is emitted as a deletion.
        """
        self._flush_unpaired_rename()

    def stats(self) -> dict[str, Any]:
        """Return state-level counters including per-event-kind breakdown."""
        result: dict[str, Any] = {
            'state_events': self._events_processed,
            'state_updates_emitted': self._updates_emitted,
            'state_deletes_emitted': self._deletes_emitted,
            'state_files': len(self.files),
            'state_emitted': len(self.emitted),
            'state_stat_calls': self._stat_calls,
            'state_stat_time': self._stat_time,
        }
        for kind_name, count in sorted(self._event_counts.items()):
            result[f'state_{kind_name}'] = count
        return result

    def _stat_path(self, path: str) -> dict[str, Any] | None:
        """Return stat metadata dict, or None on failure."""
        self._stat_calls += 1
        t0 = time.monotonic()
        try:
            stat_result = self._stat_fn(path)
            return {
                'size': stat_result.st_size,
                'uid': stat_result.st_uid,
                'gid': stat_result.st_gid,
                'mode': stat_result.st_mode,
                'atime': int(stat_result.st_atime),
                'mtime': int(stat_result.st_mtime),
                'ctime': int(stat_result.st_ctime),
            }
        except OSError:
            return None
        finally:
            self._stat_time += time.monotonic() - t0

    def _handle_created(self, event: dict[str, Any]) -> None:
        path = event['path']
        is_dir = event.get('is_dir', False)
        parent, _, name = path.rpartition('/')

        entry = {
            'path': path,
            'name': name,
            'parent': parent,
            'is_dir': is_dir,
            'is_symlink': event.get('is_symlink', False),
            'ts': event.get('ts'),
        }
        self.files[path] = entry
        self.children[parent].add(path)
        if is_dir:
            # Ensure children key exists for the new directory.
            self.children.setdefault(path, set())
        self.to_update.add(path)
        # If the path was pending deletion (e.g. removed then re-created),
        # cancel the delete — the file exists now.
        self.to_delete.discard(path)
        # Parent metadata changed (new child).
        if parent in self.files:
            self.to_update.add(parent)

    def _handle_removed(self, event: dict[str, Any]) -> None:
        path = event['path']
        entry = self.files.get(path)
        if not entry:
            return  # Ephemeral file — never tracked, nothing to do.

        parent = entry.get('parent', '')

        # If directory, recursively remove all descendants.
        if entry.get('is_dir'):
            self._remove_subtree(path)

        # Remove from parent's children.
        if parent in self.children:
            self.children[parent].discard(path)

        # Remove entry.
        self.files.pop(path, None)
        self.children.pop(path, None)
        self.to_delete.add(path)
        self.to_update.discard(path)

        # Parent metadata changed.
        if parent in self.files:
            self.to_update.add(parent)

    def _remove_subtree(self, dir_path: str) -> None:
        """Recursively remove all descendants of a directory."""
        children = list(self.children.get(dir_path, []))
        for child_path in children:
            child = self.files.get(child_path)
            if child and child.get('is_dir'):
                self._remove_subtree(child_path)
            self.files.pop(child_path, None)
            self.children.pop(child_path, None)
            self.to_delete.add(child_path)
            self.to_update.discard(child_path)
        self.children.pop(dir_path, None)

    def _handle_updated(self, event: dict[str, Any]) -> None:
        path = event['path']
        if path not in self.files:
            return  # Not tracked (e.g. pre-existing file).
        entry = self.files[path]
        if event.get('ts') is not None:
            entry['ts'] = event['ts']
        self.to_update.add(path)

    def _handle_renamed(self, event: dict[str, Any]) -> None:
        rn = self._rename
        if rn.pending is None:
            rn.pending = event
            rn.age = 0
            return

        # If the pending rename survived an emit() cycle AND its
        # path matches the last rename target, it is a displacement
        # notification that crossed a batch boundary.  Absorb it
        # and start a fresh pair with the new event.
        # (age == 0 means same batch — could be a rename chain
        # like A→B then B→C, so we must NOT absorb.)
        if rn.age > 0 and rn.pending['path'] == rn.last_target:
            rn.pending = event
            rn.age = 0
            return

        # Second rename event — complete the pair.
        old_path = rn.pending['path']
        new_path = event['path']
        rn.clear()
        rn.last_target = new_path
        self._apply_rename(old_path, new_path, event)

    def _flush_unpaired_rename(self) -> None:
        """Flush a pending rename that was never paired.

        If the pending path matches the last completed rename target,
        it is a macOS displacement notification — skip the removal.
        """
        rn = self._rename
        if rn.pending is None:
            return
        path = rn.pending['path']
        if path == rn.last_target:
            # Displacement notification — not a real removal.
            rn.clear()
            return
        event = rn.pending
        rn.clear()
        rn.last_target = None
        self._handle_removed(event)

    def _apply_rename(
        self,
        old_path: str,
        new_path: str,
        event: dict[str, Any],
    ) -> None:
        if old_path == new_path:
            entry = self.files.get(old_path)
            if entry:
                if event.get('ts') is not None:
                    entry['ts'] = event['ts']
                self.to_update.add(old_path)
            return

        entry = self.files.get(old_path)
        if not entry:
            self._rename_unknown_source(new_path, event)
            return

        self._move_entry(entry, old_path, new_path, event)

    def _rename_unknown_source(
        self,
        new_path: str,
        event: dict[str, Any],
    ) -> None:
        """Handle rename where old path was never tracked.

        Treat as creation at new_path so the file appears in output.
        """
        logger.debug(
            'Rename of unknown path -> %s (treating as create)',
            new_path,
        )
        self._handle_created(
            {
                'path': new_path,
                'is_dir': event.get('is_dir', False),
                'is_symlink': event.get('is_symlink', False),
                'ts': event.get('ts'),
            },
        )

    def _move_entry(
        self,
        entry: dict[str, Any],
        old_path: str,
        new_path: str,
        event: dict[str, Any],
    ) -> None:
        """Move a tracked entry from old_path to new_path."""
        is_dir = entry.get('is_dir', False)
        old_parent = entry.get('parent', '')
        new_parent, _, new_name = new_path.rpartition('/')

        # Handle collision: if new_path already exists, treat as deletion.
        if new_path in self.files:
            self._handle_removed(
                {'path': new_path, 'kind': EventKind.REMOVED},
            )

        # Remove from old parent.
        if old_parent in self.children:
            self.children[old_parent].discard(old_path)

        # Move entry.
        self.files.pop(old_path, None)
        entry['path'] = new_path
        entry['name'] = new_name
        entry['parent'] = new_parent
        if event.get('ts') is not None:
            entry['ts'] = event['ts']
        self.files[new_path] = entry

        # New content is at new_path now — it's an update, not a delete.
        self.to_delete.discard(new_path)

        # Add to new parent.
        self.children[new_parent].add(new_path)

        # Track updates.
        self.to_update.discard(old_path)
        self.to_update.add(new_path)
        if old_path in self.emitted:
            self.to_delete.add(old_path)
            self.emitted.add(new_path)

        # Update parent metadata.
        if old_parent in self.files:
            self.to_update.add(old_parent)
        if new_parent in self.files:
            self.to_update.add(new_parent)

        # Recursively update child paths if directory.
        if is_dir:
            self._recursively_update_paths(old_path, new_path)

    def _recursively_update_paths(
        self,
        old_prefix: str,
        new_prefix: str,
    ) -> None:
        """Update all descendant paths after a directory rename."""
        old_children = list(self.children.get(old_prefix, []))
        new_children: set[str] = set()

        for child_path in old_children:
            child = self.files.get(child_path)
            if not child:
                continue

            new_child_path = new_prefix + child_path[len(old_prefix) :]
            _, _, new_child_name = new_child_path.rpartition('/')

            # Move entry.
            self.files.pop(child_path, None)
            child['path'] = new_child_path
            child['name'] = new_child_name
            child['parent'] = new_prefix
            self.files[new_child_path] = child
            new_children.add(new_child_path)

            # Track updates.
            self.to_update.discard(child_path)
            self.to_update.add(new_child_path)
            # Clear any pending delete for the new path (e.g. rename-back).
            self.to_delete.discard(new_child_path)
            if child_path in self.emitted:
                self.to_delete.add(child_path)
                self.emitted.add(new_child_path)

            # Recurse for directory children.
            if child.get('is_dir'):
                self._recursively_update_paths(child_path, new_child_path)

        # Update children mapping.
        self.children.pop(old_prefix, None)
        self.children[new_prefix] = new_children


# Backward-compat alias
StateManager = PathStateManager
