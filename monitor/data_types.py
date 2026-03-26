"""Pydantic-based models for GPFS changelog records."""

from __future__ import annotations

from enum import StrEnum
from enum import unique


@unique
class InotifyEventType(StrEnum):
    """Inotify events emitted by GPFS mmwatch."""

    IN_ACCESS = 'IN_ACCESS'
    IN_ATTRIB = 'IN_ATTRIB'
    IN_CLOSE_NOWRITE = 'IN_CLOSE_NOWRITE'
    IN_CLOSE_WRITE = 'IN_CLOSE_WRITE'
    IN_CREATE = 'IN_CREATE'
    IN_DELETE = 'IN_DELETE'
    IN_DELETE_SELF = 'IN_DELETE_SELF'  # TODO: handle or no op
    IN_MODIFY = 'IN_MODIFY'
    IN_MOVED_FROM = 'IN_MOVED_FROM'  # TODO: handle or no op
    IN_MOVED_TO = 'IN_MOVED_TO'
    IN_MOVE_SELF = 'IN_MOVE_SELF'  # TODO: handle or no op
    IN_OPEN = 'IN_OPEN'


@unique
class EventType(StrEnum):  # TODO: rename to LustreEventType
    """An enum for Lustre changelog event types."""

    CREAT = '01CREAT'
    MKDIR = '02MKDIR'
    UNLNK = '06UNLNK'
    RMDIR = '07RMDIR'
    RENME = '08RENME'
    OPEN = '10OPEN'
    CLOSE = '11CLOSE'
    TRUNC = '13TRUNC'
    SATTR = '14SATTR'
    MTIME = '17MTIME'
    CTIME = '18CTIME'
    ATIME = '19ATIME'


INOTIFY_REDUCTION_RULES: dict[
    InotifyEventType,
    tuple[str, set[InotifyEventType]],
] = {
    InotifyEventType.IN_DELETE: ('cancel', {InotifyEventType.IN_CREATE}),
    InotifyEventType.IN_DELETE_SELF: ('cancel', {InotifyEventType.IN_CREATE}),
}


# Reduction rules similar to Robinhood's record_filters
REDUCTION_RULES: dict[EventType, tuple[str, set[EventType]]] = {
    # Data change events: IGNORE if a matching prior event is found.
    # TODO: unify the data change and metadata change events
    EventType.TRUNC: (
        'ignore',
        {EventType.TRUNC, EventType.CLOSE, EventType.MTIME, EventType.CREAT},
    ),
    EventType.CLOSE: (
        'ignore',
        {EventType.TRUNC, EventType.CLOSE, EventType.MTIME, EventType.CREAT},
    ),
    EventType.MTIME: (
        'ignore',
        {
            EventType.TRUNC,
            EventType.CLOSE,
            EventType.MTIME,
            EventType.CREAT,
            EventType.MKDIR,
        },
    ),
    # Metadata change events: IGNORE if a matching prior event is found.
    EventType.CTIME: (
        'ignore',
        {EventType.CTIME, EventType.SATTR, EventType.CREAT, EventType.MKDIR},
    ),
    EventType.ATIME: (
        'ignore',
        {EventType.CTIME, EventType.SATTR, EventType.CREAT, EventType.MKDIR},
    ),
    EventType.SATTR: (
        'ignore',
        {EventType.CTIME, EventType.SATTR, EventType.CREAT, EventType.MKDIR},
    ),
    # Deletion events: CANCEL both events if a matching prior event is found.
    EventType.UNLNK: ('cancel', {EventType.CREAT}),
    EventType.RMDIR: ('cancel', {EventType.MKDIR}),
}


class ReductionResult(StrEnum):
    """Outcomes when applying reduction rules."""

    ACCEPT = 'ACCEPT'
    DROP_NEW = 'DROP_NEW'
    DROP_NEW_AND_PREV = 'DROP_NEW_AND_PREV'  # cancel out


__all__ = ['EventType', 'InotifyEventType']
