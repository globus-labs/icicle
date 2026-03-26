"""Lustre changelog event types and line parsing for icicle."""

from __future__ import annotations

from enum import StrEnum
from enum import unique
from typing import Any

from src.icicle.events import EventKind


@unique
class LustreEventType(StrEnum):
    """Lustre changelog event types."""

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


# Map Lustre event types to unified EventKind.
LUSTRE_EVENT_MAP: dict[LustreEventType, EventKind] = {
    LustreEventType.CREAT: EventKind.CREATED,
    LustreEventType.MKDIR: EventKind.CREATED,
    LustreEventType.UNLNK: EventKind.REMOVED,
    LustreEventType.RMDIR: EventKind.REMOVED,
    LustreEventType.RENME: EventKind.RENAMED,
    LustreEventType.OPEN: EventKind.MODIFIED,
    LustreEventType.CLOSE: EventKind.MODIFIED,
    LustreEventType.TRUNC: EventKind.MODIFIED,
    LustreEventType.SATTR: EventKind.MODIFIED,
    LustreEventType.MTIME: EventKind.MODIFIED,
    LustreEventType.CTIME: EventKind.MODIFIED,
    LustreEventType.ATIME: EventKind.MODIFIED,
}

# Lustre event types that carry parent_fid + name fields.
_EVENTS_WITH_PARENT = frozenset(
    {
        LustreEventType.CREAT,
        LustreEventType.MKDIR,
        LustreEventType.UNLNK,
        LustreEventType.RMDIR,
    },
)


def _strip_brackets(token: str) -> str:
    """Remove outer brackets from a FID token like '[0x200:0x1:0x0]'."""
    if token.startswith('[') and token.endswith(']'):
        return token[1:-1]
    return token


_MIN_FIELDS = 9  # base changelog line: eid type time date flags t= ef= u= nid=


def _find_field(
    fields: list[str],
    prefix: str,
    start: int = 0,
    exclude_prefix: str | None = None,
) -> int | None:
    """Find the index of the first field starting with *prefix*.

    If *exclude_prefix* is given, skip fields that also match it
    (e.g. skip ``sp=`` when looking for ``s=``).
    """
    for i in range(start, len(fields)):
        if fields[i].startswith(prefix):
            if exclude_prefix and fields[i].startswith(exclude_prefix):
                continue
            return i
    return None


def parse_changelog_line(line: str) -> dict[str, Any] | None:  # noqa: C901
    """Parse a single Lustre changelog line into a dict.

    Returns None for empty/invalid lines and OPEN events (dropped).
    """
    raw = line.strip()
    if not raw:
        return None

    fields = raw.split()
    if len(fields) < _MIN_FIELDS:
        return None

    try:
        eid = int(fields[0])
    except ValueError:
        return None

    type_str = fields[1]
    try:
        lustre_type = LustreEventType(type_str)
    except ValueError:
        return None

    event: dict[str, Any] = {
        'eid': eid,
        'lustre_type': lustre_type,
        'ts': f'{fields[2]} {fields[3]}',
    }

    # Parse target FID (field 5: t=[...])
    if fields[5].startswith('t='):
        event['target_fid'] = _strip_brackets(fields[5][2:])
    else:
        event['target_fid'] = None

    # Parse uid:gid (field 7: u=uid:gid)
    try:
        uid_gid = fields[7].split('=', 1)[1]
        uid, gid = uid_gid.split(':')
        event['uid'] = int(uid)
        event['gid'] = int(gid)
    except (IndexError, ValueError):
        event['uid'] = 0
        event['gid'] = 0

    # Defaults for optional fields.
    event['parent_fid'] = None
    event['name'] = None
    event['source_fid'] = None
    event['source_parent_fid'] = None
    event['old_name'] = None

    # Event-specific parsing (fields 9+).
    # Names may contain spaces, so we scan for marker prefixes (p=, s=,
    # sp=) rather than using hardcoded field indices, and join remaining
    # tokens to reconstruct the full name.
    if lustre_type in _EVENTS_WITH_PARENT:
        p_idx = _find_field(fields, 'p=', start=9)
        if p_idx is not None and p_idx + 1 < len(fields):
            event['parent_fid'] = _strip_brackets(fields[p_idx][2:])
            event['name'] = ' '.join(fields[p_idx + 1 :])

    elif lustre_type == LustreEventType.RENME:
        p_idx = _find_field(fields, 'p=', start=9)
        s_idx = _find_field(fields, 's=', start=9, exclude_prefix='sp=')
        sp_idx = _find_field(fields, 'sp=', start=9)
        if p_idx is not None and s_idx is not None and sp_idx is not None:
            event['parent_fid'] = _strip_brackets(fields[p_idx][2:])
            event['name'] = ' '.join(fields[p_idx + 1 : s_idx])
            event['source_fid'] = _strip_brackets(fields[s_idx][2:])
            event['source_parent_fid'] = _strip_brackets(
                fields[sp_idx][3:],
            )
            event['old_name'] = ' '.join(fields[sp_idx + 1 :])

    return event
