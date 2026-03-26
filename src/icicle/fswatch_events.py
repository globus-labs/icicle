"""Fswatch event parsing and reduction rules for icicle."""

from __future__ import annotations

from src.icicle.batch import ReductionAction
from src.icicle.events import EventKind

# All 15 known fswatch flags (from man fswatch EVENT TYPES).
FSWATCH_FLAGS = frozenset(
    {
        'NoOp',
        'PlatformSpecific',
        'Created',
        'Updated',
        'Removed',
        'Renamed',
        'OwnerModified',
        'AttributeModified',
        'MovedFrom',
        'MovedTo',
        'IsFile',
        'IsDir',
        'IsSymLink',
        'Link',
        'Overflow',
    },
)

# Type indicator flags (not event kinds).
_TYPE_FLAGS = frozenset({'IsFile', 'IsDir', 'IsSymLink'})

_RENAME_FLAGS = frozenset({'Renamed', 'MovedFrom', 'MovedTo'})
_ATTR_FLAGS = frozenset({'AttributeModified', 'OwnerModified', 'Link'})
_IDLE_FLAGS = frozenset({'NoOp', 'Overflow'})

# Separator used in fswatch --event-flag-separator output.
FLAG_SEPARATOR = ','

# Events we ask fswatch to report (filters at source).
FSWATCH_EVENTS = (
    'Created',
    'Updated',
    'Removed',
    'Renamed',
    'AttributeModified',
    'OwnerModified',
    'MovedFrom',
    'MovedTo',
    'IsFile',
    'IsDir',
    'IsSymLink',
    'Link',
)

# Reduction rules for stat()-based backends (fswatch and Lustre).
# Since emit() calls os.stat() once per path regardless of event kind,
# any "file changed" event absorbs any other "file changed" event.
# One signal per path per batch suffices.
_ANY_CHANGE = {
    EventKind.CREATED,
    EventKind.MODIFIED,
}

STAT_REDUCTION_RULES: dict[
    EventKind,
    tuple[ReductionAction, set[EventKind]],
] = {
    # IGNORE drops the NEW event, so CREATED (old) survives when absorbed.
    EventKind.MODIFIED: (ReductionAction.IGNORE, _ANY_CHANGE),
    # Deletion cancels creation only (Robinhood alignment).
    # A modified-then-deleted file still emits the modify — but if it was
    # only created then deleted, both events are cancelled (ephemeral file).
    EventKind.REMOVED: (ReductionAction.CANCEL, {EventKind.CREATED}),
}


def classify_fswatch_flags(flags: list[str]) -> EventKind | None:
    """Map a list of fswatch flags to a single EventKind.

    Priority: Removed > Renamed > Created > Updated >
              AttributeModified/OwnerModified/Link > fallback.
    Returns None for NoOp / Overflow-only events.
    """
    flag_set = set(flags) - _TYPE_FLAGS

    result: EventKind | None = None
    if 'Removed' in flag_set:
        result = EventKind.REMOVED
    elif flag_set & _RENAME_FLAGS:
        result = EventKind.RENAMED
    elif 'Created' in flag_set:
        result = EventKind.CREATED
    elif (
        'Updated' in flag_set
        or 'PlatformSpecific' in flag_set
        or flag_set & _ATTR_FLAGS
        or flag_set - _IDLE_FLAGS
    ):
        result = EventKind.MODIFIED
    return result


def parse_fswatch_line(line: str) -> dict[str, object] | None:
    """Parse one line of fswatch output into an event dict.

    Expected format (produced by fswatch -t -f '%s' -x
    --event-flag-separator=','):
        <epoch> <path> <flag1>,<flag2>,...

    Returns dict with keys: path, kind, ts, is_dir, is_symlink,
    raw_flags.  Returns None for empty/invalid lines.
    """
    raw = line.strip()
    if not raw:
        return None

    # Split on spaces only (not tabs/newlines) to preserve
    # special whitespace in paths.
    parts = raw.split(' ')
    if not parts:
        return None

    # Last token: comma-separated flags.
    flag_str = parts[-1]
    flags = flag_str.split(FLAG_SEPARATOR)

    if not all(f in FSWATCH_FLAGS for f in flags):
        return None

    # First token: try epoch timestamp.
    ts: float | None = None
    path_start = 0
    try:
        ts = float(parts[0])
        path_start = 1
    except ValueError:
        pass

    # Middle: path (rejoin with spaces, tabs are intact).
    path = ' '.join(parts[path_start:-1])
    if not path:
        return None

    kind = classify_fswatch_flags(flags)
    if kind is None:
        return None

    return {
        'path': path,
        'kind': kind,
        'ts': ts,
        'is_dir': 'IsDir' in flags,
        'is_symlink': 'IsSymLink' in flags,
        'raw_flags': tuple(flags),
    }
