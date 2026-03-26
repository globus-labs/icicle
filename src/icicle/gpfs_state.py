"""Inode-keyed GPFS state manager for icicle."""

from __future__ import annotations

import logging
from collections import Counter
from collections import defaultdict
from pathlib import PurePosixPath
from typing import Any

from src.icicle.events import EventKind
from src.icicle.state import BaseStateManager

logger = logging.getLogger(__name__)


class GPFSStateManager(BaseStateManager):
    """Maintains an inode-keyed in-memory view of the GPFS filesystem.

    Unlike PathStateManager used by fswatch/Lustre, this manager:
    - Keys entries by fid (str(inode)) for stable identity across renames
    - Uses metadata directly from events (no os.stat calls)
    - Handles renames via single IN_MOVED_TO events (no event pairing)
    """

    def __init__(self) -> None:
        """Initialize empty filesystem state."""
        # fid -> entry dict
        self.files: dict[str, dict[str, Any]] = {}
        # path -> fid reverse lookup
        self.path_to_fid: dict[str, str] = {}
        # parent_fid -> set of child fids
        self.children: dict[str, set[str]] = defaultdict(set)

        self.to_update: set[str] = set()  # fids
        self.to_delete: set[str] = set()  # fids
        self.emitted: set[str] = set()  # fids that have been emitted

        # fid -> path at time of deletion (for emit output)
        self._delete_paths: dict[str, str] = {}

        # Counters for stats().
        self._event_counts: Counter[str] = Counter()
        self._events_processed = 0
        self._updates_emitted = 0
        self._deletes_emitted = 0

    def process_events(self, events: list[dict[str, Any]]) -> None:
        """Dispatch events to handlers by EventKind."""
        for event in events:
            kind = event.get('kind')
            if kind is not None:
                self._events_processed += 1
                self._event_counts[kind.name] += 1
            if kind == EventKind.CREATED:
                self._handle_created(event)
            elif kind == EventKind.REMOVED:
                self._handle_removed(event)
            elif kind == EventKind.RENAMED:
                self._handle_renamed(event)
            elif kind == EventKind.MODIFIED:
                self._handle_updated(event)

    def emit(self) -> list[dict[str, Any]]:
        """Return accumulated changes and clear pending state.

        Uses metadata from events (not os.stat).
        """
        changes: list[dict[str, Any]] = []

        for fid in self.to_update - self.to_delete:
            entry = self.files.get(fid)
            if not entry:
                continue
            path = entry.get('path')
            if not path:
                continue
            changes.append(
                {
                    'op': 'update',
                    'fid': fid,
                    'path': path,
                    'stat': {
                        'size': entry.get('size'),
                        'atime': entry.get('atime'),
                        'ctime': entry.get('ctime'),
                        'mtime': entry.get('mtime'),
                        'uid': entry.get('uid'),
                        'gid': entry.get('gid'),
                        'mode': entry.get('permissions'),
                    },
                },
            )
            self.emitted.add(fid)

        for fid in self.to_delete:
            if fid in self.emitted:
                path = self._delete_paths.get(fid, '')
                self.emitted.discard(fid)
                changes.append(
                    {'op': 'delete', 'fid': fid, 'path': path},
                )

        self.to_update.clear()
        self.to_delete.clear()
        self._delete_paths.clear()
        for c in changes:
            if c['op'] == 'update':
                self._updates_emitted += 1
            else:
                self._deletes_emitted += 1
        return changes

    def stats(self) -> dict[str, Any]:
        """Return state-level counters including per-event-kind breakdown."""
        result: dict[str, Any] = {
            'state_events': self._events_processed,
            'state_updates_emitted': self._updates_emitted,
            'state_deletes_emitted': self._deletes_emitted,
            'state_files': len(self.files),
            'state_emitted': len(self.emitted),
        }
        for kind_name, count in sorted(self._event_counts.items()):
            result[f'state_{kind_name}'] = count
        return result

    # ------------------------------------------------------------------
    # Event handlers
    # ------------------------------------------------------------------

    def _handle_created(self, event: dict[str, Any]) -> None:
        inode = event.get('inode')
        if inode is None:
            return

        fid = str(inode)
        event_path = event.get('path')
        if not event_path:
            return

        parent_path = str(PurePosixPath(event_path).parent)
        parent_fid = self.path_to_fid.get(parent_path)
        is_dir = bool(event.get('is_dir'))

        entry: dict[str, Any] = {
            'fid': fid,
            'parent_fid': parent_fid,
            'name': PurePosixPath(event_path).name,
            'path': event_path,
            'is_dir': is_dir,
            'size': event.get('size'),
            'atime': event.get('atime'),
            'ctime': event.get('ctime'),
            'mtime': event.get('mtime'),
            'uid': event.get('uid'),
            'gid': event.get('gid'),
            'permissions': event.get('permissions'),
        }

        self.files[fid] = entry
        self.path_to_fid[event_path] = fid
        if is_dir:
            self.children.setdefault(fid, set())
        if parent_fid:
            self.children[parent_fid].add(fid)
            self.to_update.add(parent_fid)
        self.to_update.add(fid)

    def _handle_removed(self, event: dict[str, Any]) -> None:
        inode = event.get('inode')
        if inode is None:
            return

        fid = str(inode)
        entry = self.files.get(fid)
        if not entry:
            return

        event_path = event.get('path') or ''
        self.path_to_fid.pop(event_path, None)
        self._delete_paths[fid] = entry.get('path') or ''
        self.to_delete.add(fid)

        # Update parent
        parent_fid = entry.get('parent_fid')
        if parent_fid and parent_fid in self.children:
            self.children[parent_fid].discard(fid)
            self.to_update.add(parent_fid)

        # Clean up directory children
        if entry.get('is_dir'):
            self.children.pop(fid, None)

        self.files.pop(fid, None)

    def _handle_updated(self, event: dict[str, Any]) -> None:
        inode = event.get('inode')
        if inode is None:
            return

        fid = str(inode)
        entry = self.files.get(fid)
        if not entry:
            return

        # Update metadata from event
        entry['size'] = event.get('size')
        entry['atime'] = event.get('atime')
        entry['ctime'] = event.get('ctime')
        entry['mtime'] = event.get('mtime')
        self.to_update.add(fid)

    def _handle_renamed(self, event: dict[str, Any]) -> None:
        """Handle IN_MOVED_TO: single-event rename via inode lookup."""
        inode = event.get('inode')
        if inode is None:
            return

        fid = str(inode)
        entry = self.files.get(fid)
        if not entry:
            return

        new_path = event.get('path')
        if not new_path:
            return

        # Collision detection: if new_path is occupied by a different inode,
        # synthesize a deletion for the existing entry.
        if new_path in self.path_to_fid:
            existing_fid = self.path_to_fid[new_path]
            if existing_fid != fid:
                collision_entry = self.files.get(existing_fid)
                if collision_entry:
                    collision_event: dict[str, Any] = {
                        'inode': int(existing_fid),
                        'path': collision_entry.get('path') or new_path,
                        'kind': EventKind.REMOVED,
                        'is_dir': collision_entry.get('is_dir'),
                    }
                    self._handle_removed(collision_event)

        # Remove from old parent
        old_parent_fid = entry.get('parent_fid')
        if old_parent_fid and old_parent_fid in self.children:
            self.children[old_parent_fid].discard(fid)

        # Compute new parent
        new_parent_path = str(PurePosixPath(new_path).parent)
        new_parent_fid = self.path_to_fid.get(new_parent_path)
        if new_parent_fid:
            self.children[new_parent_fid].add(fid)

        # Update entry
        old_path = entry.get('path')
        entry['path'] = new_path
        entry['name'] = PurePosixPath(new_path).name
        entry['parent_fid'] = new_parent_fid
        entry['size'] = event.get('size')
        entry['atime'] = event.get('atime')
        entry['ctime'] = event.get('ctime')
        entry['mtime'] = event.get('mtime')

        # Update path index
        self.path_to_fid.pop(old_path or '', None)
        self.path_to_fid[new_path] = fid
        self.to_update.add(fid)

        # Recursively update child paths for directory renames
        if entry.get('is_dir'):
            self._recursively_update_paths(fid)

    def _recursively_update_paths(self, parent_fid: str) -> None:
        """Update all descendant paths after a directory rename."""
        parent_entry = self.files.get(parent_fid)
        children = self.children.get(parent_fid)
        if not parent_entry or not children:
            return

        parent_path = parent_entry.get('path')
        if parent_path is None:
            return

        for child_fid in list(children):
            child_entry = self.files.get(child_fid)
            if not child_entry:
                continue

            old_path = child_entry.get('path')
            name = child_entry.get('name')
            if not name:
                continue

            new_path = str(PurePosixPath(parent_path) / name)
            child_entry['path'] = new_path
            self.path_to_fid.pop(old_path or '', None)
            self.path_to_fid[new_path] = child_fid
            self.to_update.add(child_fid)

            if child_entry.get('is_dir'):
                self._recursively_update_paths(child_fid)
