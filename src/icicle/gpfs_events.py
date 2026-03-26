"""GPFS inotify event types, mappings, and message parsing for icicle."""

from __future__ import annotations

import logging
from enum import StrEnum
from enum import unique
from typing import Any

from src.icicle.batch import ReductionAction
from src.icicle.events import EventKind

logger = logging.getLogger(__name__)


@unique
class InotifyEventType(StrEnum):
    """Inotify events emitted by GPFS mmwatch."""

    IN_ACCESS = 'IN_ACCESS'
    IN_ATTRIB = 'IN_ATTRIB'
    IN_CLOSE_NOWRITE = 'IN_CLOSE_NOWRITE'
    IN_CLOSE_WRITE = 'IN_CLOSE_WRITE'
    IN_CREATE = 'IN_CREATE'
    IN_DELETE = 'IN_DELETE'
    IN_DELETE_SELF = 'IN_DELETE_SELF'
    IN_MODIFY = 'IN_MODIFY'
    IN_MOVED_FROM = 'IN_MOVED_FROM'
    IN_MOVED_TO = 'IN_MOVED_TO'
    IN_MOVE_SELF = 'IN_MOVE_SELF'
    IN_OPEN = 'IN_OPEN'


# Map inotify event types to unified EventKind.
# None means the event is dropped at source level.
GPFS_EVENT_MAP: dict[InotifyEventType, EventKind | None] = {
    InotifyEventType.IN_CREATE: EventKind.CREATED,
    InotifyEventType.IN_DELETE: EventKind.REMOVED,
    InotifyEventType.IN_DELETE_SELF: EventKind.REMOVED,
    InotifyEventType.IN_MOVED_TO: EventKind.RENAMED,
    InotifyEventType.IN_MODIFY: EventKind.MODIFIED,
    InotifyEventType.IN_ATTRIB: EventKind.MODIFIED,
    InotifyEventType.IN_CLOSE_WRITE: EventKind.MODIFIED,
    InotifyEventType.IN_ACCESS: EventKind.MODIFIED,
    # Dropped at source — no content or metadata changes:
    InotifyEventType.IN_OPEN: None,
    InotifyEventType.IN_CLOSE_NOWRITE: None,
    # Dropped at source — IN_MOVED_TO + inode state lookup is sufficient:
    InotifyEventType.IN_MOVED_FROM: None,
    InotifyEventType.IN_MOVE_SELF: None,
}

# Events always dropped at source level (before batch processor).
GPFS_SOURCE_DROP: frozenset[InotifyEventType] = frozenset(
    {
        InotifyEventType.IN_OPEN,
        InotifyEventType.IN_CLOSE_NOWRITE,
        InotifyEventType.IN_MOVED_FROM,
        InotifyEventType.IN_MOVE_SELF,
    },
)

# GPFS reduction rules: only REMOVED cancels CREATED.
# No 'ignore' rules because GPFS uses metadata FROM events (not os.stat).
# Dropping a newer event would lose metadata accuracy.
GPFS_REDUCTION_RULES: dict[
    EventKind,
    tuple[ReductionAction, set[EventKind]],
] = {
    EventKind.REMOVED: (ReductionAction.CANCEL, {EventKind.CREATED}),
}


def parse_gpfs_message(data: dict[str, Any]) -> dict[str, Any] | None:
    """Parse a decoded GPFS mmwatch Kafka JSON message.

    Returns None for invalid messages or events that should
    be dropped at source.
    """
    if not data.get('inode') or not data.get('path') or not data.get('event'):
        return None

    # Parse the event field: may contain "IN_CREATE IN_ISDIR" etc.
    event_tokens = data['event'].split()
    is_dir = 'IN_ISDIR' in event_tokens
    non_dir_tokens = [t for t in event_tokens if t != 'IN_ISDIR']

    if not non_dir_tokens:
        return None

    try:
        event_type = InotifyEventType[non_dir_tokens[0]]
    except KeyError:
        logger.warning('unknown inotify event: %s', non_dir_tokens[0])
        return None

    # Drop events at source level.
    kind = GPFS_EVENT_MAP.get(event_type)
    if kind is None:
        return None

    return {
        'inode': int(data['inode']),
        'path': data['path'],
        'kind': kind,
        'is_dir': is_dir,
        'event_type': event_type,
        'size': data.get('fileSize'),
        'atime': data.get('atime'),
        'ctime': data.get('ctime'),
        'mtime': data.get('mtime'),
        'uid': data.get('ownerUserId'),
        'gid': data.get('ownerGroupId'),
        'permissions': data.get('permissions'),
    }
