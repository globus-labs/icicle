"""Abstract state manager interfaces for icicle2."""

from __future__ import annotations

import logging
import os
from abc import ABC
from abc import abstractmethod
from collections import defaultdict
from collections.abc import Callable
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from monitor.changelog_client import LFSClient
from monitor.data_types import EventType
from monitor.data_types import InotifyEventType

logger = logging.getLogger(__name__)


class StateManager(ABC):
    """Abstract class of StateManager."""

    def __init__(self, *, print_changelogs: bool = False) -> None:
        """Initialize counters and state stores."""
        self.print_changelogs = print_changelogs
        self.ev_received = 0
        self.ev_processed: dict[Any, int] = defaultdict(int)
        self.files: dict[str, dict[str, Any]] = {}
        self.path_to_fid: dict[str, str] = {}
        self.children: dict[str, set[str]] = defaultdict(set)
        self.to_update: set[str] = set()
        self.to_delete: set[tuple[str, str | None]] = set()
        self.emitted_updates: set[str] = set()

    def process_events(self, events: Iterable[dict[str, Any]]) -> None:
        """Process events sequentially and update counters."""
        for event in events:
            if self.print_changelogs:
                logger.info('event: %s', event)
            self.ev_received += 1
            self._handle_event(event)

            event_type = event.get('event_type')
            if isinstance(event_type, InotifyEventType):
                self.ev_processed[event_type] += 1

    @abstractmethod
    def _handle_event(self, event: dict[str, Any]) -> None:
        """Apply state transitions for a single event."""

    @abstractmethod
    def emit_updates(self) -> list[dict[str, Any]]:
        """Return state updates and reset any pending buffers."""

    @abstractmethod
    def snapshot(self) -> dict[str, Any]:
        """Return a structured view of the current state."""

    def reset_counters(self) -> None:
        """Clear event counters."""
        self.ev_received = 0
        self.ev_processed.clear()

    def _recursively_update_paths(self, parent_fid: str) -> None:
        parent_entry = self.files.get(parent_fid)
        children = self.children.get(parent_fid)
        if not parent_entry or not children:
            logger.warning(
                'Cannot update paths for parent %s: missing entry/children',
                parent_fid,
            )
            return

        parent_path = parent_entry.get('path')
        if parent_path is None:
            logger.warning(
                'Parent %s has no path; skipping recursive update.',
                parent_fid,
            )
            return

        for child_fid in list(children):
            child_entry = self.files.get(child_fid)
            if not child_entry:
                logger.warning(
                    'Child %s missing in cache for parent %s.',
                    child_fid,
                    parent_fid,
                )
                continue

            old_path = child_entry.get('path')
            name = child_entry.get('name')
            if not name:
                logger.warning('Child %s missing name; skipping.', child_fid)
                continue

            new_path = str(Path(parent_path) / name)
            child_entry['path'] = new_path
            self.path_to_fid.pop(old_path or '', None)
            self.path_to_fid[new_path] = child_fid
            self.to_update.add(child_fid)

            if child_entry.get('is_dir', False):
                self._recursively_update_paths(child_fid)

    def stats_summary(self) -> dict[str, int]:
        """Return state manager counters."""
        stats: dict[str, int] = {'sm_received': self.ev_received}
        stats.update(
            {
                f'sm_{event_type!s}': count
                for event_type, count in self.ev_processed.items()
            },
        )
        return stats


GPFSEvent = dict[str, Any]
ChangelogEvent = dict[str, Any]


class GPFSStateManager(StateManager):
    """StateManager for IBM Storage Scale."""

    def _handle_event(self, event: GPFSEvent) -> None:
        handlers: dict[InotifyEventType, Callable[[GPFSEvent], None]] = {
            InotifyEventType.IN_ACCESS: self._handle_update,
            InotifyEventType.IN_ATTRIB: self._handle_update,
            InotifyEventType.IN_CLOSE_NOWRITE: self._handle_update,
            InotifyEventType.IN_CLOSE_WRITE: self._handle_update,
            InotifyEventType.IN_CREATE: self._handle_creation,
            InotifyEventType.IN_DELETE: self._handle_deletion,
            InotifyEventType.IN_MODIFY: self._handle_update,
            InotifyEventType.IN_MOVED_TO: self._handle_rename,
            InotifyEventType.IN_OPEN: self._handle_update,
        }

        raw_event_type = event.get('event_type')
        event_type: InotifyEventType
        if isinstance(raw_event_type, InotifyEventType):
            event_type = raw_event_type
        elif isinstance(raw_event_type, str):
            try:
                event_type = InotifyEventType[raw_event_type]
            except KeyError:
                logger.warning(
                    'Ignoring event with unsupported type: %s',
                    raw_event_type,
                )
                return
            event['event_type'] = event_type
        else:
            logger.warning(
                'Ignoring event with invalid type: %s',
                raw_event_type,
            )
            return

        handler = handlers.get(event_type)
        if handler:
            handler(event)

    def emit_updates(self) -> list[dict[str, Any]]:
        """Return accumulated updates and clear pending state."""
        changes: list[dict[str, Any]] = []
        pending_deletes = {fid for fid, _ in self.to_delete}

        for fid in self.to_update - pending_deletes:
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
            self.emitted_updates.add(fid)

        for fid, path in self.to_delete:
            if fid in self.emitted_updates:
                self.emitted_updates.discard(fid)
                changes.append({'op': 'delete', 'fid': fid, 'path': path})

        self.to_update.clear()
        self.to_delete.clear()
        return changes

    def snapshot(self) -> dict[str, Any]:
        """Return a structured view of the current state."""
        return {
            'files': {fid: entry.copy() for fid, entry in self.files.items()},
            'path_to_fid': self.path_to_fid.copy(),
            'children': {
                fid: sorted(children)
                for fid, children in self.children.items()
            },
            'to_update': sorted(self.to_update),
            'to_delete': list(self.to_delete),
            'emitted_updates': list(self.emitted_updates),
        }

    def _handle_creation(self, event: GPFSEvent) -> None:
        inode = event.get('inode')
        if inode is None:
            logger.warning('Creation event missing inode: %s', event)
            return

        fid = str(inode)
        event_path = event.get('path')
        if not event_path:
            logger.warning('Creation event missing path for fid %s', fid)
            return

        parent_path = Path(event_path).parent
        parent_fid = self.path_to_fid.get(str(parent_path))
        is_dir = bool(event.get('is_dir'))

        entry: dict[str, Any] = {
            'fid': fid,
            'parent_fid': parent_fid,
            'name': Path(event_path).name,
            'path': event_path,
            'is_dir': is_dir,
            'atime': event.get('atime'),
            'ctime': event.get('ctime'),
            'mtime': event.get('mtime'),
            'size': event.get('size'),
            'permissions': event.get('permissions'),
            'uid': event.get('uid'),
            'gid': event.get('gid'),
        }

        self.files[fid] = entry
        self.path_to_fid[event_path] = fid
        if is_dir:
            self.children[fid]
        if parent_fid:
            self.children[parent_fid].add(fid)
        self.to_update.add(fid)

    def _handle_deletion(self, event: GPFSEvent) -> None:
        inode = event.get('inode')
        if inode is None:
            logger.debug('Deletion event missing inode: %s', event)
            return

        fid = str(inode)
        entry = self.files.get(fid)
        if not entry:
            logger.debug('deletion of unknown fid %s', fid)
            return

        event_path = event.get('path') or ''
        self.path_to_fid.pop(event_path, None)
        self.to_delete.add((fid, entry.get('path')))

        parent_fid = entry.get('parent_fid')
        if parent_fid and parent_fid in self.children:
            self.children[parent_fid].discard(fid)
            self.to_update.add(parent_fid)

        if entry.get('is_dir'):
            children = self.children.get(fid)
            if children:
                logger.debug(
                    'deleting non-empty directory %s',
                    fid,
                )  # due to out of order in multiple partitions
            self.children.pop(fid, None)

        self.files.pop(fid, None)

    def _handle_update(self, event: GPFSEvent) -> None:
        inode = event.get('inode')
        if inode is None:
            logger.debug('Update event missing inode: %s', event)
            return

        fid = str(inode)
        entry = self.files.get(fid)
        if not entry:
            logger.debug('update for unknown fid %s', fid)
            return

        entry['size'] = event.get('size')
        entry['atime'] = event.get('atime')
        entry['ctime'] = event.get('ctime')
        entry['mtime'] = event.get('mtime')
        self.to_update.add(fid)

    def _handle_rename(self, event: GPFSEvent) -> None:
        inode = event.get('inode')
        if inode is None:
            logger.debug('Rename event missing inode: %s', event)
            return

        fid = str(inode)
        entry = self.files.get(fid)
        if not entry:
            logger.debug('rename for unknown fid %s', fid)
            return

        new_path = event.get('path')
        if not new_path:
            logger.debug('Rename event missing path for fid %s', fid)
            return

        if new_path in self.path_to_fid:
            existing_fid = self.path_to_fid[new_path]
            assert existing_fid
            if existing_fid != fid:
                collision_entry = self.files.get(existing_fid)
                assert collision_entry, (
                    f'rename collision missing cached entry for {existing_fid}'
                )
                collision_path = collision_entry.get('path') or new_path

                assert existing_fid.isdigit(), (
                    f'rename collision fid {existing_fid} not numeric'
                )
                collision_inode = int(existing_fid)

                collision_event: dict[str, Any] = {
                    'eid': event.get('eid'),
                    'event_type': InotifyEventType.IN_DELETE,
                    'inode': collision_inode,
                    'path': collision_path,
                    'is_dir': collision_entry.get('is_dir'),
                    'size': collision_entry.get('size')
                    if collision_entry.get('size') is not None
                    else event.get('size'),
                    'atime': collision_entry.get('atime')
                    or event.get('atime'),
                    'ctime': collision_entry.get('ctime')
                    or event.get('ctime'),
                    'mtime': collision_entry.get('mtime')
                    or event.get('mtime'),
                    'permissions': event.get('permissions'),
                    'uid': event.get('uid'),
                    'gid': event.get('gid'),
                }
                self._handle_deletion(collision_event)

        previous_parent_fid = entry.get('parent_fid')
        if previous_parent_fid and previous_parent_fid in self.children:
            self.children[previous_parent_fid].discard(fid)

        parent_path = str(Path(new_path).parent)
        parent_fid = self.path_to_fid.get(parent_path)
        if parent_fid:
            self.children[parent_fid].add(fid)

        old_path = entry.get('path')
        entry['path'] = new_path
        entry['name'] = Path(new_path).name
        entry['parent_fid'] = parent_fid
        entry['size'] = event.get('size')
        entry['atime'] = event.get('atime')
        entry['ctime'] = event.get('ctime')
        entry['mtime'] = event.get('mtime')

        self.path_to_fid.pop(old_path or '', None)
        self.path_to_fid[new_path] = fid
        self.to_update.add(fid)

        if entry.get('is_dir'):
            self._recursively_update_paths(fid)


class LFSStateManager(StateManager):
    """A class for managing file paths."""

    def __init__(
        self,
        mount_point: str,
        lfs_client: LFSClient,
        print_changelogs: bool,
        fid_resolution_method: str,
    ):
        """Initialise the path manager."""
        super().__init__(print_changelogs=print_changelogs)
        self.mount_point: str = mount_point
        self.lfs_client: LFSClient = lfs_client
        self.fid_resolution_method = fid_resolution_method
        self.events: list[dict[str, Any]] = []

    def process_events(self, events: Iterable[ChangelogEvent]) -> None:
        """Process Lustre events sequentially and update counters."""
        for event in events:
            event_payload = event.copy()
            if self.print_changelogs:
                logger.info('State Manager Event: %s', event_payload)
            self.events.append(event_payload)
            self.ev_received += 1
            self._handle_event(event_payload)

            event_type = event_payload.get('event_type')
            if isinstance(event_type, EventType):  # TODO: merge
                self.ev_processed[event_type] += 1

    def _handle_event(self, event: ChangelogEvent) -> None:
        """Dispatch Lustre events to the appropriate handler."""
        handlers: dict[EventType, Callable[[ChangelogEvent], None]] = {
            EventType.CREAT: lambda e: self._handle_creation(e, is_dir=False),
            EventType.MKDIR: lambda e: self._handle_creation(e, is_dir=True),
            EventType.UNLNK: self._handle_deletion,
            EventType.RMDIR: self._handle_deletion,
            EventType.RENME: self._handle_rename,
            EventType.TRUNC: self._handle_update,
            EventType.SATTR: self._handle_update,
            EventType.MTIME: self._handle_update,
            EventType.CTIME: self._handle_update,
            EventType.ATIME: self._handle_update,
        }

        raw_event_type = event.get('event_type')
        if isinstance(raw_event_type, EventType):
            event_type = raw_event_type
        elif isinstance(raw_event_type, str):
            try:
                event_type = EventType(raw_event_type)
            except ValueError:
                logger.warning(
                    'Ignoring event with unsupported type: %s',
                    raw_event_type,
                )
                return
            event['event_type'] = event_type
        else:
            logger.warning(
                'Ignoring event with invalid type: %s',
                raw_event_type,
            )
            return

        handler = handlers.get(event_type)
        if handler:
            handler(event)

    def _build_update_payload(
        self,
        fid: str,
        path: str | None,
    ) -> dict[str, Any] | None:
        """Build the metadata dictionary for an updated file."""
        assert path
        try:
            stat_info = os.stat(path)
            stat_dict = {
                'mode': stat_info.st_mode,
                # 'nlink': stat_info.st_nlink,
                'uid': stat_info.st_uid,
                'gid': stat_info.st_gid,
                'size': stat_info.st_size,
                'atime': int(stat_info.st_atime),
                'mtime': int(stat_info.st_mtime),
                'ctime': int(stat_info.st_ctime),
            }
            return {'fid': fid, 'path': path, 'stat': stat_dict}
        except (OSError, FileNotFoundError) as e:
            # TODO: ./parallel_run.sh 100 0 0 3 evaluate_output triggers this
            # because the batch is full?
            # how should we handle? currently the correctness is not affected.
            logger.debug(f'Failed to stat path {path} for FID {fid}: {e}')
            return None

    def emit_updates(self) -> list[dict[str, Any]]:
        """Get metadata updates and reset the state."""
        metadata_updates: list[dict[str, Any]] = []
        fids_to_delete = {fid for fid, _ in self.to_delete}

        for fid in self.to_update - fids_to_delete:
            entry = self.files.get(fid)
            entry_path = entry.get('path') if entry else None
            if entry and entry_path:
                payload = self._build_update_payload(fid, entry_path)
                if payload:
                    payload['op'] = 'update'
                    metadata_updates.append(payload)
                    self.emitted_updates.add(fid)

        for fid, path in self.to_delete:
            if fid in self.emitted_updates:
                metadata_updates.append(
                    {'op': 'delete', 'fid': fid, 'path': path},
                )
                self.emitted_updates.discard(fid)

        self.to_update.clear()
        self.to_delete.clear()
        self.events.clear()
        # self.lfs_client.fid2path_calls.clear()
        return metadata_updates

    def get_updates_and_reset(self) -> list[dict[str, Any]]:
        """Backward-compatible alias for emit_updates."""
        return self.emit_updates()

    def snapshot(self) -> dict[str, Any]:
        """Return a structured snapshot of the Lustre cache."""
        return {
            'files': {fid: entry.copy() for fid, entry in self.files.items()},
            'path_to_fid': self.path_to_fid.copy(),
            'children': {
                fid: sorted(children)
                for fid, children in self.children.items()
            },
            'to_update': sorted(self.to_update),
            'to_delete': list(self.to_delete),
            'emitted_updates': list(self.emitted_updates),
            'events': self.events.copy(),
        }

    def get_full_status_and_reset(self) -> dict[str, Any]:
        """Get complete status of the state manager and reset the states."""
        status: dict[str, Any] = {
            'summary': {
                'events': len(self.events),
                # 'fid2path_calls': len(self.lfs_client.fid2path_calls),
                'files': len(self.files),
                'path_to_fid': len(self.path_to_fid),
                'children': len(self.children),
                'to_update': len(self.to_update),
                'to_delete': len(self.to_delete),
            },
            'events': self.events.copy(),
            # 'fid2path_calls': self.lfs_client.fid2path_calls.copy(),
            'files': {fid: entry.copy() for fid, entry in self.files.items()},
            'path_to_fid': self.path_to_fid.copy(),
            'children': {p: list(c) for p, c in self.children.items()},
            'to_update': [],
            'to_delete': [],
        }

        fids_to_delete = {fid for fid, _ in self.to_delete}
        for fid in self.to_update - fids_to_delete:
            entry = self.files.get(fid)
            entry_path = entry.get('path') if entry else None
            if entry and entry_path:
                payload = self._build_update_payload(fid, entry_path)
                if payload:
                    self.emitted_updates.add(fid)
                    status['to_update'].append(
                        [payload['fid'], payload['path'], payload['stat']],
                    )

        for fid, path in self.to_delete:
            if fid in self.emitted_updates:
                self.emitted_updates.discard(fid)
                status['to_delete'].append([fid, path])

        self.to_update.clear()
        self.to_delete.clear()
        self.events.clear()
        # self.lfs_client.fid2path_calls.clear()
        return status

    def _handle_creation(
        self,
        event: ChangelogEvent,
        is_dir: bool = False,
    ) -> None:
        """Handle a file or directory creation event."""
        target_fid = event['target_fid']
        parent_fid = event['parent_fid']
        name = event['name']
        assert target_fid
        assert parent_fid
        assert name

        parent = self.files.get(parent_fid)
        parent_path = parent.get('path') if parent else None
        if not parent or not parent_path:
            parent_path = self.lfs_client.fid2path(parent_fid)
            if not parent_path:
                logger.debug(  # use debug for mu mode + naive
                    "Could not resolve parent FID %s for new FID '%s'. "
                    'Parent may have been deleted before its creation was processed.',  # noqa: E501
                    parent_fid,
                    target_fid,
                )
                return

            parent = {
                'fid': parent_fid,
                'parent_fid': None,
                'path': parent_path,
                'name': Path(parent_path).name,
                'is_dir': True,
            }
            self.files[parent_fid] = parent
            self.path_to_fid[parent_path] = parent_fid
            self.children[parent_fid] = set()
        parent_path = parent.get('path') if parent else None
        self.to_update.add(parent_fid)

        # 2. New entry
        new_entry_path: str | None
        if self.fid_resolution_method == 'icicle':
            assert parent_path
            new_entry_path = str(Path(parent_path).joinpath(name))
        else:  # fsmonitor, naive
            new_entry_path = self.lfs_client.fid2path(target_fid)
            if not new_entry_path:
                logger.debug(
                    "Could not resolve entry path new FID '%s'. ",
                    target_fid,
                )
                return
                # in case of create-renme-unlink, this file is not created
                # still need to handle old and new parents metadata updates
                # TODO: maybe we allow no update for this?

        if new_entry_path in self.path_to_fid:
            logger.warning(
                'Path %s already exists in cache. Should not happen',
                new_entry_path,
            )

        assert new_entry_path
        entry = {
            'fid': target_fid,
            'parent_fid': parent_fid,
            'name': name,
            'is_dir': is_dir,
            'path': new_entry_path,
        }
        self.files[target_fid] = entry
        self.path_to_fid[new_entry_path] = target_fid
        if is_dir:
            self.children[target_fid] = set()
        self.to_update.add(target_fid)

        # 3. Update the parent-child relationship.
        self.children[parent_fid].add(target_fid)

    def _handle_deletion(self, event: ChangelogEvent) -> None:
        """Handle a deletion event."""
        target_fid = event['target_fid']
        parent_fid = event['parent_fid']
        assert target_fid
        assert parent_fid

        fid = target_fid
        if fid not in self.files:
            logger.debug(f'Deletion event for untracked FID: {fid}')
            return

        entry = self.files[fid]

        # 1. Queue the entry for downstream deletion.
        entry_path = entry.get('path')
        if entry_path:
            self.path_to_fid.pop(entry_path, None)
            self.to_delete.add((fid, entry_path))
        else:
            # This occurs if a path could not be resolved earlier, typically
            # due to a rapid `rename -> unlink` sequence where the `fid2path`
            # call during rename processing failed because the file was gone.
            # evaluate_output.sh could also trigger this.
            logger.debug(
                f'No path found for deletion of FID {fid}. Marking as "FileRemoved".',  # noqa: E501
            )
            self.to_delete.add((fid, 'FileRemoved'))

        if entry.get('is_dir'):
            if self.children.get(fid):
                logger.debug(  # TODO in filebench
                    'Deleting directory %s that is not empty in cache. '
                    'May indicate missed child unlink events.',
                    fid,
                )
            self.children.pop(fid, None)

        # 2. Update parent state.
        parent = self.files.get(parent_fid)
        if not parent:
            # This is an anomaly. A deletion event should always have a parent
            # that is tracked in the cache. If not, it may indicate missed
            # prior events (e.g., a missed MKDIR for the parent).
            # new:
            # rapid ops in evaluate_output trigger this
            logger.debug(
                'Parent FID %s not found for deletion of child FID %s.',
                parent_fid,
                fid,
            )
        else:
            # Remove the child from the parent's children set.
            if parent_fid in self.children:
                self.children[parent_fid].discard(fid)

            # Queue the parent for an update as its contents have changed.
            if parent.get('path'):
                self.to_update.add(parent_fid)

        # 3. Finally, remove the entry from the cache.
        del self.files[fid]

    def _handle_update(self, event: ChangelogEvent) -> None:
        """Handle an update event."""
        target_fid = event['target_fid']
        assert target_fid

        fid = target_fid
        if fid not in self.files:
            logger.debug(f'Modification event for unknown FID: {fid}')
            return

        entry = self.files[fid]
        if not entry.get('path'):
            logger.debug(
                f'No path found for deletion of FID {fid}.',
            )
        self.to_update.add(fid)

    def _recursively_update_paths(self, parent_fid: str) -> None:  # noqa: C901, PLR0912
        """Recursively updates the paths for all descendants of a given parent FID."""  # noqa: E501
        parent_entry = self.files.get(parent_fid)
        if not parent_entry or parent_fid not in self.children:
            logger.warning(
                'Cannot recursively update paths for directory FID %s: not found in state cache.',  # noqa: E501
                parent_fid,
            )
            return

        for child_fid in self.children[parent_fid]:
            child_entry = self.files.get(child_fid)
            if not child_entry:
                logger.warning(
                    'Descendant FID %s found in children of %s but not in main file cache. Skipping.',  # noqa: E501
                    child_fid,
                    parent_fid,
                )
                continue

            old_child_path = child_entry.get('path')
            new_child_path = None

            # Note: FSMonitor does not implement this because it does not
            # track previous encountered file path as we do
            # this improves FSMonitor's performance in handling
            # rename events in evaluation
            if self.fid_resolution_method in ('icicle', 'fsmonitor'):
                parent_path = parent_entry.get('path')
                if not parent_path:
                    logger.warning(
                        'Parent FID %s has an invalid path; cannot construct new path for child FID %s.',  # noqa: E501
                        parent_fid,
                        child_fid,
                    )
                    # Path remains invalid, will be handled by pop/add logic below # noqa: E501
                elif not child_entry.get('name'):
                    logger.warning(
                        'Child FID %s is missing a name. Cannot construct new path.',  # noqa: E501
                        child_fid,
                    )
                else:
                    new_child_path = str(
                        Path(parent_path).joinpath(child_entry['name']),
                    )
            else:  # On-demand resolution
                new_child_path = self.lfs_client.fid2path(child_fid)
                if not new_child_path:
                    logger.warning(
                        'fid2path failed to resolve path for child FID %s during recursive update.',  # noqa: E501
                        child_fid,
                    )

            child_entry['path'] = new_child_path
            if new_child_path:
                child_entry['name'] = Path(new_child_path).name

            # Update the main path-to-FID index
            if old_child_path:
                self.path_to_fid.pop(old_child_path, None)
            if new_child_path:
                self.path_to_fid[new_child_path] = child_fid

            self.to_update.add(child_fid)

            if child_entry.get('is_dir'):
                self._recursively_update_paths(child_fid)

    def _handle_rename(self, event: ChangelogEvent) -> None:  # noqa: C901, PLR0912, PLR0915
        """Handle a rename event."""
        target_fid = event['target_fid']
        source_fid = event['source_fid']
        source_parent_fid = event['source_parent_fid']
        parent_fid = event['parent_fid']
        name = event['name']
        assert target_fid is not None
        assert source_fid is not None
        assert source_parent_fid is not None
        assert parent_fid is not None
        assert name

        # An overwrite is a rename where the target is an existing file.
        if target_fid != '0:0x0:0x0':
            self._handle_deletion(event)

        src_fid = source_fid

        # Update parent relationships
        if _ := self.files.get(source_parent_fid):
            if source_parent_fid in self.children:
                self.children[source_parent_fid].discard(src_fid)
            self.to_update.add(source_parent_fid)
        else:
            logger.debug(
                'Old parent FID %s not found in cache during rename of child FID %s. '  # noqa: E501
                "Cannot update old parent's state.",
                source_parent_fid,
                src_fid,
            )

        if new_parent := self.files.get(parent_fid):
            if parent_fid in self.children:
                self.children[parent_fid].add(src_fid)
            self.to_update.add(parent_fid)
        else:
            # rapid ops in evaluate_output trigger this
            logger.debug(
                'New parent FID %s not found in cache for rename of child FID %s. '  # noqa: E501
                "Cannot update new parent's state.",
                parent_fid,
                src_fid,
            )

        entry = self.files.get(src_fid)
        if not entry:
            logger.debug(f'Rename event for untracked source FID: {src_fid}')
            return

        old_path = entry.get('path')
        if not old_path:
            logger.warning(
                f'Original path missing for rename of FID {src_fid}. Skipping rename.',  # noqa: E501
            )
            return

        # naive fid resolution's encountering renme -> unlnk sequence
        # when issuing fid2path, cannot find the path
        # because the file has been deleted (does not harm the outer logic)
        # icicle fid resolution: concatenate when possible
        # less resolution call

        entry['parent_fid'] = parent_fid
        entry['name'] = name
        entry['path'] = None  # Invalidate path before recalculating

        # Compute the new path based on the resolution method
        if self.fid_resolution_method in ('icicle', 'fsmonitor'):
            new_parent = self.files.get(parent_fid)
            if new_parent and new_parent.get('path'):
                entry['path'] = str(
                    Path(new_parent['path']).joinpath(name),
                )
            else:
                # rapid ops in evaluate_output trigger this
                logger.debug(
                    'New parent FID %s for renamed FID %s is not valid. Marking path as invalid.',  # noqa: E501
                    parent_fid,
                    src_fid,
                )
        else:  # On-demand resolution
            entry['path'] = self.lfs_client.fid2path(source_fid)
            if not entry['path']:
                # This is expected in rapid rename->unlink sequences.
                logger.debug(
                    'fid2path failed for renamed FID %s, likely because it was quickly deleted.',  # noqa: E501
                    src_fid,
                )

        self.path_to_fid.pop(old_path, None)
        new_path = entry.get('path')
        if new_path:
            self.path_to_fid[new_path] = src_fid
            self.to_update.add(src_fid)

        if entry.get('is_dir'):
            self._recursively_update_paths(src_fid)


__all__ = ['GPFSStateManager', 'LFSStateManager', 'StateManager']
