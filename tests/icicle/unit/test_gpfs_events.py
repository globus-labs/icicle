"""Tests for GPFS event types, mapping, parsing, and reduction rules."""

from __future__ import annotations

from src.icicle.events import EventKind
from src.icicle.gpfs_events import GPFS_EVENT_MAP
from src.icicle.gpfs_events import GPFS_REDUCTION_RULES
from src.icicle.gpfs_events import GPFS_SOURCE_DROP
from src.icicle.gpfs_events import InotifyEventType
from src.icicle.gpfs_events import parse_gpfs_message

# ---------------------------------------------------------------------------
# InotifyEventType enum
# ---------------------------------------------------------------------------


class TestInotifyEventType:
    def test_all_12_members(self):
        assert len(InotifyEventType) == 12

    def test_values_match_names(self):
        for member in InotifyEventType:
            assert member.value == member.name

    def test_lookup_by_name(self):
        assert InotifyEventType['IN_CREATE'] == InotifyEventType.IN_CREATE

    def test_lookup_by_value(self):
        assert InotifyEventType('IN_MODIFY') == InotifyEventType.IN_MODIFY


# ---------------------------------------------------------------------------
# GPFS_EVENT_MAP
# ---------------------------------------------------------------------------


class TestGPFSEventMap:
    def test_all_types_mapped(self):
        for et in InotifyEventType:
            assert et in GPFS_EVENT_MAP

    def test_create_maps_to_created(self):
        assert GPFS_EVENT_MAP[InotifyEventType.IN_CREATE] == EventKind.CREATED

    def test_delete_maps_to_removed(self):
        assert GPFS_EVENT_MAP[InotifyEventType.IN_DELETE] == EventKind.REMOVED
        assert (
            GPFS_EVENT_MAP[InotifyEventType.IN_DELETE_SELF]
            == EventKind.REMOVED
        )

    def test_moved_to_maps_to_renamed(self):
        assert (
            GPFS_EVENT_MAP[InotifyEventType.IN_MOVED_TO] == EventKind.RENAMED
        )

    def test_modify_maps_to_modified(self):
        assert GPFS_EVENT_MAP[InotifyEventType.IN_MODIFY] == EventKind.MODIFIED
        assert GPFS_EVENT_MAP[InotifyEventType.IN_ATTRIB] == EventKind.MODIFIED
        assert (
            GPFS_EVENT_MAP[InotifyEventType.IN_CLOSE_WRITE]
            == EventKind.MODIFIED
        )

    def test_access_maps_to_accessed(self):
        assert GPFS_EVENT_MAP[InotifyEventType.IN_ACCESS] == EventKind.MODIFIED

    def test_dropped_events_map_to_none(self):
        assert GPFS_EVENT_MAP[InotifyEventType.IN_OPEN] is None
        assert GPFS_EVENT_MAP[InotifyEventType.IN_CLOSE_NOWRITE] is None
        assert GPFS_EVENT_MAP[InotifyEventType.IN_MOVED_FROM] is None
        assert GPFS_EVENT_MAP[InotifyEventType.IN_MOVE_SELF] is None


# ---------------------------------------------------------------------------
# Source-level drop set
# ---------------------------------------------------------------------------


class TestSourceDrop:
    def test_drop_set_matches_none_mappings(self):
        none_types = {
            et for et, kind in GPFS_EVENT_MAP.items() if kind is None
        }
        assert none_types == GPFS_SOURCE_DROP


# ---------------------------------------------------------------------------
# GPFS_REDUCTION_RULES
# ---------------------------------------------------------------------------


class TestGPFSReductionRules:
    def test_only_removed_cancels_created(self):
        assert len(GPFS_REDUCTION_RULES) == 1
        action, targets = GPFS_REDUCTION_RULES[EventKind.REMOVED]
        assert action == 'cancel'
        assert targets == {EventKind.CREATED}

    def test_no_ignore_rules(self):
        for kind, (action, _) in GPFS_REDUCTION_RULES.items():
            assert action != 'ignore'

    def test_modified_not_in_rules(self):
        assert EventKind.MODIFIED not in GPFS_REDUCTION_RULES


# ---------------------------------------------------------------------------
# parse_gpfs_message
# ---------------------------------------------------------------------------


class TestParseGpfsMessage:
    def test_basic_file_create(self):
        data = {
            'inode': 12345,
            'path': '/gpfs/fs1/file.txt',
            'event': 'IN_CREATE',
            'fileSize': 1024,
            'atime': 1700000000,
            'ctime': 1700000001,
            'mtime': 1700000002,
            'ownerUserId': 1000,
            'ownerGroupId': 1000,
            'permissions': '0644',
        }
        result = parse_gpfs_message(data)
        assert result is not None
        assert result['inode'] == 12345
        assert result['path'] == '/gpfs/fs1/file.txt'
        assert result['kind'] == EventKind.CREATED
        assert result['is_dir'] is False
        assert result['size'] == 1024
        assert result['uid'] == 1000
        assert result['gid'] == 1000

    def test_dir_create_with_isdir(self):
        data = {
            'inode': 100,
            'path': '/gpfs/fs1/subdir',
            'event': 'IN_CREATE IN_ISDIR',
            'fileSize': 0,
        }
        result = parse_gpfs_message(data)
        assert result is not None
        assert result['is_dir'] is True
        assert result['kind'] == EventKind.CREATED

    def test_delete_event(self):
        data = {
            'inode': 200,
            'path': '/gpfs/fs1/old.txt',
            'event': 'IN_DELETE',
        }
        result = parse_gpfs_message(data)
        assert result is not None
        assert result['kind'] == EventKind.REMOVED

    def test_moved_to_event(self):
        data = {
            'inode': 300,
            'path': '/gpfs/fs1/new_name.txt',
            'event': 'IN_MOVED_TO',
        }
        result = parse_gpfs_message(data)
        assert result is not None
        assert result['kind'] == EventKind.RENAMED

    def test_modify_event(self):
        data = {
            'inode': 400,
            'path': '/gpfs/fs1/file.txt',
            'event': 'IN_MODIFY',
            'fileSize': 2048,
            'mtime': 1700000010,
        }
        result = parse_gpfs_message(data)
        assert result is not None
        assert result['kind'] == EventKind.MODIFIED
        assert result['size'] == 2048

    def test_dropped_event_returns_none(self):
        for event_str in (
            'IN_OPEN',
            'IN_CLOSE_NOWRITE',
            'IN_MOVED_FROM',
            'IN_MOVE_SELF',
        ):
            data = {'inode': 1, 'path': '/a', 'event': event_str}
            assert parse_gpfs_message(data) is None

    def test_missing_inode_returns_none(self):
        data = {'inode': None, 'path': '/a', 'event': 'IN_CREATE'}
        assert parse_gpfs_message(data) is None

    def test_missing_path_returns_none(self):
        data = {'inode': 1, 'path': '', 'event': 'IN_CREATE'}
        assert parse_gpfs_message(data) is None

    def test_missing_event_returns_none(self):
        data = {'inode': 1, 'path': '/a', 'event': ''}
        assert parse_gpfs_message(data) is None

    def test_unknown_event_returns_none(self):
        data = {'inode': 1, 'path': '/a', 'event': 'IN_UNKNOWN'}
        assert parse_gpfs_message(data) is None

    def test_isdir_only_returns_none(self):
        data = {'inode': 1, 'path': '/a', 'event': 'IN_ISDIR'}
        assert parse_gpfs_message(data) is None

    def test_metadata_fields_optional(self):
        data = {'inode': 1, 'path': '/a', 'event': 'IN_CREATE'}
        result = parse_gpfs_message(data)
        assert result is not None
        assert result['size'] is None
        assert result['atime'] is None
        assert result['uid'] is None

    def test_multiple_non_dir_tokens_uses_first(self):
        """Multiple event tokens — first non-ISDIR token determines kind."""
        data = {'inode': 1, 'path': '/a', 'event': 'IN_MODIFY IN_CLOSE_WRITE'}
        result = parse_gpfs_message(data)
        assert result is not None
        # First non-ISDIR token is IN_MODIFY
        assert result['kind'] == EventKind.MODIFIED
        assert result['is_dir'] is False

    def test_dir_create_with_extra_tokens(self):
        data = {'inode': 1, 'path': '/a', 'event': 'IN_CREATE IN_ISDIR'}
        result = parse_gpfs_message(data)
        assert result is not None
        assert result['kind'] == EventKind.CREATED
        assert result['is_dir'] is True

    def test_dir_delete_with_isdir(self):
        data = {'inode': 1, 'path': '/a', 'event': 'IN_DELETE IN_ISDIR'}
        result = parse_gpfs_message(data)
        assert result is not None
        assert result['kind'] == EventKind.REMOVED
        assert result['is_dir'] is True

    def test_inode_converted_to_int(self):
        """Inode field should be converted to int regardless of input type."""
        data = {'inode': '12345', 'path': '/a', 'event': 'IN_CREATE'}
        result = parse_gpfs_message(data)
        # inode='12345' is truthy, so it passes the guard
        assert result is not None
        assert result['inode'] == 12345

    def test_zero_inode_returns_none(self):
        """Inode 0 is falsy — should return None."""
        data = {'inode': 0, 'path': '/a', 'event': 'IN_CREATE'}
        result = parse_gpfs_message(data)
        assert result is None

    def test_all_metadata_fields_preserved(self):
        """All optional metadata fields are passed through."""
        data = {
            'inode': 42,
            'path': '/test',
            'event': 'IN_MODIFY',
            'fileSize': 999,
            'atime': 100,
            'ctime': 200,
            'mtime': 300,
            'ownerUserId': 1000,
            'ownerGroupId': 2000,
            'permissions': '0755',
        }
        result = parse_gpfs_message(data)
        assert result['size'] == 999
        assert result['atime'] == 100
        assert result['ctime'] == 200
        assert result['mtime'] == 300
        assert result['uid'] == 1000
        assert result['gid'] == 2000
        assert result['permissions'] == '0755'
