"""Tests for Lustre changelog event parsing."""

from __future__ import annotations

import pytest

from src.icicle.events import EventKind
from src.icicle.lustre_events import LUSTRE_EVENT_MAP
from src.icicle.lustre_events import LustreEventType
from src.icicle.lustre_events import parse_changelog_line

# ---------------------------------------------------------------------------
# Real changelog lines captured from live Lustre cluster (fs0-MDT0000)
# ---------------------------------------------------------------------------
LINE_SATTR = (
    '1 14SATTR 02:10:01.138251424 2026.03.16'
    ' 0x44 t=[0x200000007:0x1:0x0] ef=0xf u=0:0'
    ' nid=172.31.39.56@tcp'
)
LINE_CREAT = (
    '4 01CREAT 02:10:01.749034600 2026.03.16'
    ' 0x0 t=[0x200000401:0x1:0x0] ef=0xf u=0:0'
    ' nid=172.31.39.56@tcp p=[0x200000007:0x1:0x0] test.txt'
)
LINE_OPEN = (
    '5 10OPEN 02:10:01.749071801 2026.03.16'
    ' 0x24a t=[0x200000401:0x1:0x0] ef=0xf u=0:0'
    ' nid=172.31.39.56@tcp m=-w- p=[0x200000007:0x1:0x0]'
)
LINE_CLOSE = (
    '6 11CLOSE 02:10:01.750006074 2026.03.16'
    ' 0x242 t=[0x200000401:0x1:0x0] ef=0xf u=0:0'
    ' nid=172.31.39.56@tcp'
)
LINE_MKDIR = (
    '9 02MKDIR 02:52:19.794688065 2026.03.16'
    ' 0x0 t=[0x200000401:0x2:0x0] ef=0xf u=1000:1000'
    ' nid=172.31.39.56@tcp p=[0x200000007:0x1:0x0] test_plan_explore'
)
LINE_CREAT_NESTED = (
    '10 01CREAT 02:52:19.797308841 2026.03.16'
    ' 0x0 t=[0x200000401:0x3:0x0] ef=0xf u=1000:1000'
    ' nid=172.31.39.56@tcp p=[0x200000401:0x2:0x0] file1.txt'
)
LINE_RENME = (
    '16 08RENME 02:52:19.805772855 2026.03.16'
    ' 0x0 t=[0:0x0:0x0] ef=0xf u=1000:1000'
    ' nid=172.31.39.56@tcp p=[0x200000401:0x2:0x0] file1_renamed.txt'
    ' s=[0x200000401:0x3:0x0] sp=[0x200000401:0x2:0x0] file1.txt'
)
LINE_UNLNK = (
    '17 06UNLNK 02:52:19.807645282 2026.03.16'
    ' 0x1 t=[0x200000401:0x4:0x0] ef=0xf u=1000:1000'
    ' nid=172.31.39.56@tcp p=[0x200000401:0x2:0x0] file2.txt'
)
LINE_RMDIR = (
    '23 07RMDIR 02:52:19.813302595 2026.03.16'
    ' 0x1 t=[0x200000401:0x2:0x0] ef=0xf u=1000:1000'
    ' nid=172.31.39.56@tcp p=[0x200000007:0x1:0x0] test_plan_explore'
)


# ===================================================================
# parse_changelog_line — CREAT
# ===================================================================
class TestParseCreat:
    def test_basic_fields(self) -> None:
        ev = parse_changelog_line(LINE_CREAT)
        assert ev is not None
        assert ev['eid'] == 4
        assert ev['lustre_type'] == LustreEventType.CREAT
        assert ev['target_fid'] == '0x200000401:0x1:0x0'
        assert ev['uid'] == 0
        assert ev['gid'] == 0

    def test_parent_and_name(self) -> None:
        ev = parse_changelog_line(LINE_CREAT)
        assert ev is not None
        assert ev['parent_fid'] == '0x200000007:0x1:0x0'
        assert ev['name'] == 'test.txt'

    def test_timestamp(self) -> None:
        ev = parse_changelog_line(LINE_CREAT)
        assert ev is not None
        assert ev['ts'] == '02:10:01.749034600 2026.03.16'

    def test_nested_file(self) -> None:
        ev = parse_changelog_line(LINE_CREAT_NESTED)
        assert ev is not None
        assert ev['eid'] == 10
        assert ev['parent_fid'] == '0x200000401:0x2:0x0'
        assert ev['name'] == 'file1.txt'
        assert ev['uid'] == 1000
        assert ev['gid'] == 1000


# ===================================================================
# parse_changelog_line — MKDIR
# ===================================================================
class TestParseMkdir:
    def test_basic_fields(self) -> None:
        ev = parse_changelog_line(LINE_MKDIR)
        assert ev is not None
        assert ev['eid'] == 9
        assert ev['lustre_type'] == LustreEventType.MKDIR
        assert ev['target_fid'] == '0x200000401:0x2:0x0'

    def test_parent_and_name(self) -> None:
        ev = parse_changelog_line(LINE_MKDIR)
        assert ev is not None
        assert ev['parent_fid'] == '0x200000007:0x1:0x0'
        assert ev['name'] == 'test_plan_explore'


# ===================================================================
# parse_changelog_line — UNLNK
# ===================================================================
class TestParseUnlnk:
    def test_basic_fields(self) -> None:
        ev = parse_changelog_line(LINE_UNLNK)
        assert ev is not None
        assert ev['eid'] == 17
        assert ev['lustre_type'] == LustreEventType.UNLNK
        assert ev['target_fid'] == '0x200000401:0x4:0x0'

    def test_parent_and_name(self) -> None:
        ev = parse_changelog_line(LINE_UNLNK)
        assert ev is not None
        assert ev['parent_fid'] == '0x200000401:0x2:0x0'
        assert ev['name'] == 'file2.txt'


# ===================================================================
# parse_changelog_line — RMDIR
# ===================================================================
class TestParseRmdir:
    def test_basic_fields(self) -> None:
        ev = parse_changelog_line(LINE_RMDIR)
        assert ev is not None
        assert ev['eid'] == 23
        assert ev['lustre_type'] == LustreEventType.RMDIR
        assert ev['parent_fid'] == '0x200000007:0x1:0x0'
        assert ev['name'] == 'test_plan_explore'


# ===================================================================
# parse_changelog_line — RENME
# ===================================================================
class TestParseRenme:
    def test_basic_fields(self) -> None:
        ev = parse_changelog_line(LINE_RENME)
        assert ev is not None
        assert ev['eid'] == 16
        assert ev['lustre_type'] == LustreEventType.RENME
        assert ev['target_fid'] == '0:0x0:0x0'

    def test_new_location(self) -> None:
        ev = parse_changelog_line(LINE_RENME)
        assert ev is not None
        assert ev['parent_fid'] == '0x200000401:0x2:0x0'
        assert ev['name'] == 'file1_renamed.txt'

    def test_old_location(self) -> None:
        ev = parse_changelog_line(LINE_RENME)
        assert ev is not None
        assert ev['source_fid'] == '0x200000401:0x3:0x0'
        assert ev['source_parent_fid'] == '0x200000401:0x2:0x0'
        assert ev['old_name'] == 'file1.txt'


# ===================================================================
# parse_changelog_line — OPEN (no longer hardcoded as dropped)
# ===================================================================
class TestParseOpen:
    def test_open_is_parsed(self) -> None:
        ev = parse_changelog_line(LINE_OPEN)
        assert ev is not None
        assert ev['lustre_type'] == LustreEventType.OPEN

    def test_open_with_read_mode(self) -> None:
        line = (
            '7 10OPEN 02:10:01.864644161 2026.03.16'
            ' 0x1 t=[0x200000401:0x1:0x0] ef=0xf u=1000:1000'
            ' nid=172.31.39.56@tcp m=r-- p=[0x200000007:0x1:0x0]'
        )
        ev = parse_changelog_line(line)
        assert ev is not None
        assert ev['lustre_type'] == LustreEventType.OPEN


# ===================================================================
# parse_changelog_line — CLOSE, SATTR, TRUNC, MTIME, CTIME, ATIME
# ===================================================================
class TestParseMetadataEvents:
    def test_close(self) -> None:
        ev = parse_changelog_line(LINE_CLOSE)
        assert ev is not None
        assert ev['lustre_type'] == LustreEventType.CLOSE
        assert ev['eid'] == 6
        assert ev['target_fid'] == '0x200000401:0x1:0x0'
        # CLOSE has no parent_fid/name.
        assert ev['parent_fid'] is None
        assert ev['name'] is None

    def test_sattr(self) -> None:
        ev = parse_changelog_line(LINE_SATTR)
        assert ev is not None
        assert ev['lustre_type'] == LustreEventType.SATTR
        assert ev['eid'] == 1
        assert ev['target_fid'] == '0x200000007:0x1:0x0'

    def test_mtime(self) -> None:
        line = (
            '30 17MTIME 03:00:00.000000000 2026.03.16'
            ' 0x0 t=[0x200000401:0x5:0x0] ef=0xf u=1000:1000'
            ' nid=172.31.39.56@tcp'
        )
        ev = parse_changelog_line(line)
        assert ev is not None
        assert ev['lustre_type'] == LustreEventType.MTIME

    def test_ctime(self) -> None:
        line = (
            '31 18CTIME 03:00:00.000000000 2026.03.16'
            ' 0x0 t=[0x200000401:0x5:0x0] ef=0xf u=1000:1000'
            ' nid=172.31.39.56@tcp'
        )
        ev = parse_changelog_line(line)
        assert ev is not None
        assert ev['lustre_type'] == LustreEventType.CTIME

    def test_atime(self) -> None:
        line = (
            '32 19ATIME 03:00:00.000000000 2026.03.16'
            ' 0x0 t=[0x200000401:0x5:0x0] ef=0xf u=1000:1000'
            ' nid=172.31.39.56@tcp'
        )
        ev = parse_changelog_line(line)
        assert ev is not None
        assert ev['lustre_type'] == LustreEventType.ATIME

    def test_trunc(self) -> None:
        line = (
            '33 13TRUNC 03:00:00.000000000 2026.03.16'
            ' 0x0 t=[0x200000401:0x5:0x0] ef=0xf u=1000:1000'
            ' nid=172.31.39.56@tcp'
        )
        ev = parse_changelog_line(line)
        assert ev is not None
        assert ev['lustre_type'] == LustreEventType.TRUNC


# ===================================================================
# parse_changelog_line — edge cases
# ===================================================================
class TestParseEdgeCases:
    def test_empty_string(self) -> None:
        assert parse_changelog_line('') is None

    def test_whitespace_only(self) -> None:
        assert parse_changelog_line('   \n') is None

    def test_too_few_fields(self) -> None:
        assert parse_changelog_line('1 01CREAT 02:10:01') is None

    def test_non_numeric_eid(self) -> None:
        line = (
            'abc 01CREAT 02:10:01.000 2026.03.16'
            ' 0x0 t=[0x200000401:0x1:0x0] ef=0xf u=0:0'
            ' nid=172.31.39.56@tcp p=[0x200000007:0x1:0x0] test.txt'
        )
        assert parse_changelog_line(line) is None

    def test_unknown_event_type(self) -> None:
        line = (
            '1 99XYZZY 02:10:01.000 2026.03.16'
            ' 0x0 t=[0x200000401:0x1:0x0] ef=0xf u=0:0'
            ' nid=172.31.39.56@tcp'
        )
        assert parse_changelog_line(line) is None

    def test_creat_without_parent_fields(self) -> None:
        """CREAT with only 9 base fields (missing parent_fid/name)."""
        line = (
            '1 01CREAT 02:10:01.000 2026.03.16'
            ' 0x0 t=[0x200000401:0x1:0x0] ef=0xf u=0:0'
            ' nid=172.31.39.56@tcp'
        )
        ev = parse_changelog_line(line)
        assert ev is not None
        assert ev['parent_fid'] is None
        assert ev['name'] is None

    def test_renme_without_source_fields(self) -> None:
        """RENME with fewer than 14 fields — partial parse."""
        line = (
            '1 08RENME 02:10:01.000 2026.03.16'
            ' 0x0 t=[0:0x0:0x0] ef=0xf u=0:0'
            ' nid=172.31.39.56@tcp p=[0x200000401:0x2:0x0] newname'
        )
        ev = parse_changelog_line(line)
        assert ev is not None
        assert ev['parent_fid'] is None  # not parsed without full fields
        assert ev['source_fid'] is None


# ===================================================================
# LUSTRE_EVENT_MAP
# ===================================================================
class TestLustreEventMap:
    @pytest.mark.parametrize(
        ('lustre_type', 'expected_kind'),
        [
            (LustreEventType.CREAT, EventKind.CREATED),
            (LustreEventType.MKDIR, EventKind.CREATED),
            (LustreEventType.UNLNK, EventKind.REMOVED),
            (LustreEventType.RMDIR, EventKind.REMOVED),
            (LustreEventType.RENME, EventKind.RENAMED),
            (LustreEventType.CLOSE, EventKind.MODIFIED),
            (LustreEventType.TRUNC, EventKind.MODIFIED),
            (LustreEventType.SATTR, EventKind.MODIFIED),
            (LustreEventType.MTIME, EventKind.MODIFIED),
            (LustreEventType.CTIME, EventKind.MODIFIED),
            (LustreEventType.ATIME, EventKind.MODIFIED),
        ],
    )
    def test_mapping(
        self,
        lustre_type: LustreEventType,
        expected_kind: EventKind,
    ) -> None:
        assert LUSTRE_EVENT_MAP[lustre_type] == expected_kind

    def test_open_mapped_to_modified(self) -> None:
        assert LUSTRE_EVENT_MAP[LustreEventType.OPEN] == EventKind.MODIFIED

    def test_all_types_mapped(self) -> None:
        for lt in LustreEventType:
            assert lt in LUSTRE_EVENT_MAP, (
                f'{lt} missing from LUSTRE_EVENT_MAP'
            )
