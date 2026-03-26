"""Tests for icicle event types and fswatch line parsing."""

from __future__ import annotations

import pytest

from src.icicle.events import EventKind
from src.icicle.fswatch_events import classify_fswatch_flags
from src.icicle.fswatch_events import parse_fswatch_line

# ---------------------------------------------------------------------------
# classify_fswatch_flags
# ---------------------------------------------------------------------------


class TestClassifyFswatchFlags:
    def test_created_file(self):
        assert (
            classify_fswatch_flags(['Created', 'IsFile']) == EventKind.CREATED
        )

    def test_created_dir(self):
        assert (
            classify_fswatch_flags(['Created', 'IsDir']) == EventKind.CREATED
        )

    def test_updated_file(self):
        assert (
            classify_fswatch_flags(['Updated', 'IsFile']) == EventKind.MODIFIED
        )

    def test_removed_file(self):
        assert (
            classify_fswatch_flags(['Removed', 'IsFile']) == EventKind.REMOVED
        )

    def test_removed_dir(self):
        assert (
            classify_fswatch_flags(['Removed', 'IsDir']) == EventKind.REMOVED
        )

    def test_renamed_file(self):
        assert (
            classify_fswatch_flags(['Renamed', 'IsFile']) == EventKind.RENAMED
        )

    def test_renamed_dir(self):
        assert (
            classify_fswatch_flags(['Renamed', 'IsDir']) == EventKind.RENAMED
        )

    def test_attr_modified(self):
        assert (
            classify_fswatch_flags(['AttributeModified', 'IsFile'])
            == EventKind.MODIFIED
        )

    def test_owner_modified(self):
        assert (
            classify_fswatch_flags(['OwnerModified', 'IsFile'])
            == EventKind.MODIFIED
        )

    def test_symlink_created(self):
        assert (
            classify_fswatch_flags(['Created', 'IsSymLink'])
            == EventKind.CREATED
        )

    def test_link_count_changed(self):
        assert classify_fswatch_flags(['Link', 'IsFile']) == EventKind.MODIFIED

    def test_noop_returns_none(self):
        assert classify_fswatch_flags(['NoOp']) is None

    def test_overflow_returns_none(self):
        assert classify_fswatch_flags(['Overflow']) is None

    def test_platform_specific(self):
        assert (
            classify_fswatch_flags(['PlatformSpecific', 'IsFile'])
            == EventKind.MODIFIED
        )

    def test_moved_from(self):
        assert (
            classify_fswatch_flags(['MovedFrom', 'IsFile'])
            == EventKind.RENAMED
        )

    def test_moved_to(self):
        assert (
            classify_fswatch_flags(['MovedTo', 'IsFile']) == EventKind.RENAMED
        )

    # Priority tests
    def test_removed_wins_over_created(self):
        assert (
            classify_fswatch_flags(['Created', 'Removed', 'IsFile'])
            == EventKind.REMOVED
        )

    def test_renamed_wins_over_updated(self):
        assert (
            classify_fswatch_flags(['Updated', 'Renamed', 'IsFile'])
            == EventKind.RENAMED
        )

    def test_created_wins_over_updated(self):
        assert (
            classify_fswatch_flags(
                ['Created', 'Updated', 'AttributeModified', 'IsFile'],
            )
            == EventKind.CREATED
        )

    def test_removed_wins_over_everything(self):
        assert (
            classify_fswatch_flags(
                ['Created', 'Updated', 'Removed', 'Renamed', 'IsFile'],
            )
            == EventKind.REMOVED
        )

    def test_empty_flags_returns_none(self):
        assert classify_fswatch_flags([]) is None

    def test_only_type_flags_returns_none(self):
        assert classify_fswatch_flags(['IsFile']) is None

    def test_all_flags_combined(self):
        """When every known flag is present, Removed wins (highest priority)."""
        from src.icicle.fswatch_events import FSWATCH_FLAGS

        all_flags = list(FSWATCH_FLAGS)
        assert classify_fswatch_flags(all_flags) == EventKind.REMOVED


# ---------------------------------------------------------------------------
# parse_fswatch_line — new format: <epoch> <path> <flags,comma,separated>
# ---------------------------------------------------------------------------


class TestParseFswatchLine:
    def test_parse_created_file(self):
        line = '1710423106 /tmp/test/file.txt Created,IsFile'
        ev = parse_fswatch_line(line)
        assert ev is not None
        assert ev['path'] == '/tmp/test/file.txt'
        assert ev['kind'] == EventKind.CREATED
        assert ev['is_dir'] is False
        assert ev['is_symlink'] is False

    def test_parse_created_dir(self):
        line = '1710423106 /tmp/test/subdir Created,IsDir'
        ev = parse_fswatch_line(line)
        assert ev is not None
        assert ev['kind'] == EventKind.CREATED
        assert ev['is_dir'] is True

    def test_parse_updated_file(self):
        line = '1710423106 /tmp/test/file.txt Updated,IsFile'
        ev = parse_fswatch_line(line)
        assert ev is not None
        assert ev['kind'] == EventKind.MODIFIED

    def test_parse_removed_file(self):
        line = '1710423106 /tmp/test/file.txt Removed,IsFile'
        ev = parse_fswatch_line(line)
        assert ev is not None
        assert ev['kind'] == EventKind.REMOVED

    def test_parse_removed_dir(self):
        line = '1710423106 /tmp/test/subdir Removed,IsDir'
        ev = parse_fswatch_line(line)
        assert ev is not None
        assert ev['kind'] == EventKind.REMOVED
        assert ev['is_dir'] is True

    def test_parse_renamed_file(self):
        line = '1710423106 /tmp/test/old.txt Renamed,IsFile'
        ev = parse_fswatch_line(line)
        assert ev is not None
        assert ev['kind'] == EventKind.RENAMED

    def test_parse_renamed_dir(self):
        line = '1710423106 /tmp/test/olddir Renamed,IsDir'
        ev = parse_fswatch_line(line)
        assert ev is not None
        assert ev['kind'] == EventKind.RENAMED
        assert ev['is_dir'] is True

    def test_parse_attr_modified(self):
        line = '1710423106 /tmp/test/file.txt AttributeModified,IsFile'
        ev = parse_fswatch_line(line)
        assert ev is not None
        assert ev['kind'] == EventKind.MODIFIED

    def test_parse_owner_modified(self):
        line = '1710423106 /tmp/test/file.txt OwnerModified,IsFile'
        ev = parse_fswatch_line(line)
        assert ev is not None
        assert ev['kind'] == EventKind.MODIFIED

    def test_parse_symlink_created(self):
        line = '1710423106 /tmp/test/link.txt Created,IsSymLink'
        ev = parse_fswatch_line(line)
        assert ev is not None
        assert ev['kind'] == EventKind.CREATED
        assert ev['is_symlink'] is True

    def test_parse_link_count_changed(self):
        line = '1710423106 /tmp/test/file.txt Link,IsFile'
        ev = parse_fswatch_line(line)
        assert ev is not None
        assert ev['kind'] == EventKind.MODIFIED

    def test_parse_noop(self):
        line = '1710423106 /tmp/test NoOp'
        ev = parse_fswatch_line(line)
        assert ev is None

    def test_parse_overflow(self):
        line = '1710423106 /tmp/test Overflow'
        ev = parse_fswatch_line(line)
        assert ev is None

    def test_parse_moved_from(self):
        line = '1710423106 /tmp/test/file.txt MovedFrom,IsFile'
        ev = parse_fswatch_line(line)
        assert ev is not None
        assert ev['kind'] == EventKind.RENAMED

    def test_parse_moved_to(self):
        line = '1710423106 /tmp/test/file.txt MovedTo,IsFile'
        ev = parse_fswatch_line(line)
        assert ev is not None
        assert ev['kind'] == EventKind.RENAMED

    # Multi-flag / priority
    def test_coalesced_created_updated_attr(self):
        line = '1710423106 /tmp/test/file.txt Created,Updated,AttributeModified,IsFile'
        ev = parse_fswatch_line(line)
        assert ev is not None
        assert ev['kind'] == EventKind.CREATED

    def test_removed_wins_over_created(self):
        line = '1710423106 /tmp/test/file.txt Created,Removed,IsFile'
        ev = parse_fswatch_line(line)
        assert ev is not None
        assert ev['kind'] == EventKind.REMOVED

    def test_renamed_wins_over_updated(self):
        line = '1710423106 /tmp/test/file.txt Updated,Renamed,IsFile'
        ev = parse_fswatch_line(line)
        assert ev is not None
        assert ev['kind'] == EventKind.RENAMED

    # Timestamp
    def test_timestamp_unix_float(self):
        line = '1710423106 /tmp/test/file.txt Created,IsFile'
        ev = parse_fswatch_line(line)
        assert ev is not None
        assert ev['ts'] == 1710423106.0

    def test_no_timestamp(self):
        # Line without epoch prefix (just path + flags)
        line = '/tmp/test/file.txt Created,IsFile'
        ev = parse_fswatch_line(line)
        assert ev is not None
        assert ev['ts'] is None
        assert ev['path'] == '/tmp/test/file.txt'
        assert ev['kind'] == EventKind.CREATED

    # Edge cases
    def test_empty_line(self):
        assert parse_fswatch_line('') is None
        assert parse_fswatch_line('   ') is None

    def test_malformed_line(self):
        assert parse_fswatch_line('x') is None
        assert parse_fswatch_line('just some random text') is None

    def test_path_with_spaces(self):
        line = '1710423106 /tmp/my dir/my file.txt Created,IsFile'
        ev = parse_fswatch_line(line)
        assert ev is not None
        assert ev['path'] == '/tmp/my dir/my file.txt'
        assert ev['kind'] == EventKind.CREATED

    def test_raw_flags_preserved(self):
        line = '1710423106 /tmp/test/file.txt Created,Updated,IsFile'
        ev = parse_fswatch_line(line)
        assert ev is not None
        assert 'Created' in ev['raw_flags']
        assert 'Updated' in ev['raw_flags']
        assert 'IsFile' in ev['raw_flags']

    def test_unicode_path(self):
        line = '1710423106 /tmp/datos/archivo.txt Created,IsFile'
        ev = parse_fswatch_line(line)
        assert ev is not None
        assert ev['path'] == '/tmp/datos/archivo.txt'
        assert ev['kind'] == EventKind.CREATED

    def test_very_long_path(self):
        long_path = '/' + 'a' * 4096
        line = f'1710423106 {long_path} Created,IsFile'
        ev = parse_fswatch_line(line)
        assert ev is not None
        assert len(ev['path']) == 4097

    def test_timestamp_zero(self):
        line = '0 /tmp/file.txt Created,IsFile'
        ev = parse_fswatch_line(line)
        assert ev is not None
        assert ev['ts'] == 0.0
        assert ev['path'] == '/tmp/file.txt'

    def test_fractional_timestamp(self):
        line = '1710423106.123 /tmp/file.txt Updated,IsFile'
        ev = parse_fswatch_line(line)
        assert ev is not None
        assert ev['ts'] == pytest.approx(1710423106.123)


class TestRejectsLegacyFormat:
    """Legacy space-separated format is no longer supported."""

    def test_legacy_ctime_format_rejected(self):
        line = 'Sat Mar 14 15:11:48 2026 /tmp/test/file.txt Created IsFile'
        assert parse_fswatch_line(line) is None

    def test_legacy_no_timestamp_rejected(self):
        line = '/tmp/test/file.txt Created IsFile'
        assert parse_fswatch_line(line) is None


class TestParseFswatchLineAdditional:
    def test_path_ending_with_slash(self):
        line = '1710423106 /tmp/dir/ Created,IsDir'
        ev = parse_fswatch_line(line)
        assert ev is not None
        assert ev['path'] == '/tmp/dir/'

    def test_very_large_timestamp(self):
        line = '4102444800 /tmp/future.txt Created,IsFile'
        ev = parse_fswatch_line(line)
        assert ev is not None
        assert ev['ts'] == 4102444800.0

    def test_only_type_flags_returns_none(self):
        """Only IsFile — no event kind, returns None."""
        line = '1710423106 /tmp/file.txt IsFile'
        assert parse_fswatch_line(line) is None

    def test_multi_flag_priority(self):
        """Created+Updated+AttributeModified → Created wins."""
        line = '1710423106 /tmp/f Created,Updated,AttributeModified,IsFile'
        ev = parse_fswatch_line(line)
        assert ev['kind'] == EventKind.CREATED
        assert len(ev['raw_flags']) == 4


class TestClassifyFlagsAdditional:
    def test_moved_from_moved_to(self):
        assert (
            classify_fswatch_flags(['MovedFrom', 'MovedTo', 'IsFile'])
            == EventKind.RENAMED
        )

    def test_all_attr_flags(self):
        assert (
            classify_fswatch_flags(
                ['AttributeModified', 'OwnerModified', 'Link', 'IsFile'],
            )
            == EventKind.MODIFIED
        )

    def test_removed_wins_over_renamed(self):
        assert (
            classify_fswatch_flags(['Removed', 'Renamed', 'IsFile'])
            == EventKind.REMOVED
        )

    def test_created_wins_over_attr(self):
        assert (
            classify_fswatch_flags(['Created', 'AttributeModified', 'IsFile'])
            == EventKind.CREATED
        )

    def test_platform_specific_alone(self):
        assert (
            classify_fswatch_flags(['PlatformSpecific']) == EventKind.MODIFIED
        )


class TestEventKindEnum:
    def test_all_kinds_are_strings(self):
        for kind in EventKind:
            assert isinstance(kind, str)

    def test_exactly_4_members(self):
        assert len(EventKind) == 4
