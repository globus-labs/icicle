"""Tests for LustreChangelogSource with mocked subprocess."""

from __future__ import annotations

from typing import Any
from unittest.mock import call
from unittest.mock import MagicMock
from unittest.mock import patch

from src.icicle.events import EventKind
from src.icicle.lustre_source import LustreChangelogSource


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _make_source(**kwargs: Any) -> LustreChangelogSource:
    defaults = {
        'mdt': 'fs0-MDT0000',
        'mount_point': '/mnt/fs0',
        'fsname': 'fs0',
        'poll_interval': 0.0,  # no delay in tests
        'startrec': 0,
    }
    defaults.update(kwargs)
    return LustreChangelogSource(**defaults)


def _mock_popen(lines: str) -> MagicMock:
    """Return a mock Popen whose communicate() returns (lines, '')."""
    proc = MagicMock()
    proc.communicate.return_value = (lines, '')
    proc.returncode = 0
    return proc


def _mock_fid2path(path: str) -> MagicMock:
    """Return a mock subprocess.run result for fid2path returning *path*."""
    result = MagicMock()
    result.stdout = path + '\n'
    result.returncode = 0
    return result


# Reusable changelog lines.
CREAT_DIR = (
    '1 02MKDIR 02:52:19.794 2026.03.16'
    ' 0x0 t=[0x200000401:0x2:0x0] ef=0xf u=1000:1000'
    ' nid=172.31.39.56@tcp p=[0x200000007:0x1:0x0] mydir'
)
CREAT_FILE = (
    '2 01CREAT 02:52:19.797 2026.03.16'
    ' 0x0 t=[0x200000401:0x3:0x0] ef=0xf u=1000:1000'
    ' nid=172.31.39.56@tcp p=[0x200000401:0x2:0x0] file1.txt'
)
OPEN_FILE = (
    '3 10OPEN 02:52:19.797 2026.03.16'
    ' 0x24a t=[0x200000401:0x3:0x0] ef=0xf u=1000:1000'
    ' nid=172.31.39.56@tcp m=-w- p=[0x200000401:0x2:0x0]'
)
CLOSE_FILE = (
    '4 11CLOSE 02:52:19.798 2026.03.16'
    ' 0x242 t=[0x200000401:0x3:0x0] ef=0xf u=1000:1000'
    ' nid=172.31.39.56@tcp'
)
RENME_FILE = (
    '5 08RENME 02:52:19.805 2026.03.16'
    ' 0x0 t=[0:0x0:0x0] ef=0xf u=1000:1000'
    ' nid=172.31.39.56@tcp p=[0x200000401:0x2:0x0] renamed.txt'
    ' s=[0x200000401:0x3:0x0] sp=[0x200000401:0x2:0x0] file1.txt'
)
UNLNK_FILE = (
    '6 06UNLNK 02:52:19.807 2026.03.16'
    ' 0x1 t=[0x200000401:0x3:0x0] ef=0xf u=1000:1000'
    ' nid=172.31.39.56@tcp p=[0x200000401:0x2:0x0] renamed.txt'
)
RMDIR_DIR = (
    '7 07RMDIR 02:52:19.813 2026.03.16'
    ' 0x1 t=[0x200000401:0x2:0x0] ef=0xf u=1000:1000'
    ' nid=172.31.39.56@tcp p=[0x200000007:0x1:0x0] mydir'
)
SATTR_FILE = (
    '8 14SATTR 02:52:20.000 2026.03.16'
    ' 0x44 t=[0x200000401:0x3:0x0] ef=0xf u=1000:1000'
    ' nid=172.31.39.56@tcp'
)
MTIME_FILE = (
    '9 17MTIME 02:52:20.100 2026.03.16'
    ' 0x0 t=[0x200000401:0x3:0x0] ef=0xf u=1000:1000'
    ' nid=172.31.39.56@tcp'
)


# ===================================================================
# FID cache population
# ===================================================================
class TestFidCachePopulation:
    @patch('src.icicle.lustre_source.subprocess.run')
    @patch('src.icicle.lustre_source.subprocess.Popen')
    def test_mkdir_populates_cache(
        self, mock_popen: MagicMock, mock_run: MagicMock,
    ) -> None:
        mock_popen.return_value = _mock_popen(CREAT_DIR + '\n')
        mock_run.side_effect = [_mock_fid2path('/')]
        src = _make_source()
        events = src.read()

        assert len(events) == 1
        assert events[0]['path'] == '/mnt/fs0/mydir'
        assert events[0]['kind'] == EventKind.CREATED
        assert events[0]['is_dir'] is True
        # FID cached.
        assert src._fid_to_path['0x200000401:0x2:0x0'] == '/mnt/fs0/mydir'
        assert src._fid_is_dir['0x200000401:0x2:0x0'] is True

    @patch('src.icicle.lustre_source.subprocess.run')
    @patch('src.icicle.lustre_source.subprocess.Popen')
    def test_creat_populates_cache(
        self, mock_popen: MagicMock, mock_run: MagicMock,
    ) -> None:
        mock_popen.return_value = _mock_popen(
            CREAT_DIR + '\n' + CREAT_FILE + '\n',
        )
        mock_run.side_effect = [_mock_fid2path('/')]
        src = _make_source()
        events = src.read()

        assert len(events) == 2
        assert events[1]['path'] == '/mnt/fs0/mydir/file1.txt'
        assert events[1]['kind'] == EventKind.CREATED
        assert events[1]['is_dir'] is False
        assert (
            src._fid_to_path['0x200000401:0x3:0x0']
            == '/mnt/fs0/mydir/file1.txt'
        )

    def test_caches_start_empty(self) -> None:
        src = _make_source()
        assert src._fid_to_path == {}
        assert src._fid_is_dir == {}


# ===================================================================
# FID cache update on RENME
# ===================================================================
class TestFidCacheRename:
    @patch('src.icicle.lustre_source.subprocess.run')
    @patch('src.icicle.lustre_source.subprocess.Popen')
    def test_rename_updates_cache(
        self, mock_popen: MagicMock, mock_run: MagicMock,
    ) -> None:
        lines = CREAT_DIR + '\n' + CREAT_FILE + '\n' + RENME_FILE + '\n'
        mock_popen.return_value = _mock_popen(lines)
        mock_run.side_effect = [_mock_fid2path('/')]
        src = _make_source()
        events = src.read()

        renamed_events = [e for e in events if e['kind'] == EventKind.RENAMED]
        assert len(renamed_events) == 2
        assert renamed_events[0]['path'] == '/mnt/fs0/mydir/file1.txt'
        assert renamed_events[1]['path'] == '/mnt/fs0/mydir/renamed.txt'
        assert (
            src._fid_to_path['0x200000401:0x3:0x0']
            == '/mnt/fs0/mydir/renamed.txt'
        )

    @patch('src.icicle.lustre_source.subprocess.run')
    @patch('src.icicle.lustre_source.subprocess.Popen')
    def test_rename_produces_paired_events(
        self, mock_popen: MagicMock, mock_run: MagicMock,
    ) -> None:
        lines = CREAT_DIR + '\n' + CREAT_FILE + '\n' + RENME_FILE + '\n'
        mock_popen.return_value = _mock_popen(lines)
        mock_run.side_effect = [_mock_fid2path('/')]
        src = _make_source()
        events = src.read()

        renamed = [e for e in events if e['kind'] == EventKind.RENAMED]
        assert len(renamed) == 2
        assert renamed[0]['path'] == '/mnt/fs0/mydir/file1.txt'
        assert renamed[1]['path'] == '/mnt/fs0/mydir/renamed.txt'


# ===================================================================
# FID cache cleanup on UNLNK/RMDIR
# ===================================================================
class TestFidCacheDelete:
    @patch('src.icicle.lustre_source.subprocess.run')
    @patch('src.icicle.lustre_source.subprocess.Popen')
    def test_unlnk_removes_from_cache(
        self, mock_popen: MagicMock, mock_run: MagicMock,
    ) -> None:
        lines = CREAT_DIR + '\n' + CREAT_FILE + '\n' + UNLNK_FILE + '\n'
        mock_popen.return_value = _mock_popen(lines)
        mock_run.side_effect = [_mock_fid2path('/')]
        src = _make_source()
        events = src.read()

        removed = [e for e in events if e['kind'] == EventKind.REMOVED]
        assert len(removed) == 1
        assert removed[0]['path'] == '/mnt/fs0/mydir/file1.txt'
        assert '0x200000401:0x3:0x0' not in src._fid_to_path

    @patch('src.icicle.lustre_source.subprocess.run')
    @patch('src.icicle.lustre_source.subprocess.Popen')
    def test_rmdir_removes_from_cache(
        self, mock_popen: MagicMock, mock_run: MagicMock,
    ) -> None:
        lines = CREAT_DIR + '\n' + RMDIR_DIR + '\n'
        mock_popen.return_value = _mock_popen(lines)
        mock_run.side_effect = [_mock_fid2path('/')]
        src = _make_source()
        events = src.read()

        removed = [e for e in events if e['kind'] == EventKind.REMOVED]
        assert len(removed) == 1
        assert removed[0]['path'] == '/mnt/fs0/mydir'
        assert removed[0]['is_dir'] is True
        assert '0x200000401:0x2:0x0' not in src._fid_to_path

    @patch('src.icicle.lustre_source.subprocess.run')
    @patch('src.icicle.lustre_source.subprocess.Popen')
    def test_unlnk_uses_parent_name_when_not_in_cache(
        self, mock_popen: MagicMock, mock_run: MagicMock,
    ) -> None:
        """UNLNK for a FID not in cache — resolve via parent_fid + name."""
        unlnk_unknown = (
            '10 06UNLNK 02:52:19.807 2026.03.16'
            ' 0x1 t=[0xDEAD:0x1:0x0] ef=0xf u=1000:1000'
            ' nid=172.31.39.56@tcp p=[0x200000401:0x2:0x0] orphan.txt'
        )
        lines = CREAT_DIR + '\n' + unlnk_unknown + '\n'
        mock_popen.return_value = _mock_popen(lines)
        mock_run.side_effect = [_mock_fid2path('/')]
        src = _make_source()
        events = src.read()

        removed = [e for e in events if e['kind'] == EventKind.REMOVED]
        assert len(removed) == 1
        assert removed[0]['path'] == '/mnt/fs0/mydir/orphan.txt'


# ===================================================================
# fid2path fallback
# ===================================================================
class TestFid2pathFallback:
    @patch('src.icicle.lustre_source.subprocess.run')
    @patch('src.icicle.lustre_source.subprocess.Popen')
    def test_fid2path_called_on_cache_miss(
        self, mock_popen: MagicMock, mock_run: MagicMock,
    ) -> None:
        """When parent FID is not in cache, fid2path is called."""
        creat_unknown_parent = (
            '1 01CREAT 02:52:19.797 2026.03.16'
            ' 0x0 t=[0x200000401:0x9:0x0] ef=0xf u=1000:1000'
            ' nid=172.31.39.56@tcp p=[0xUNKNOWN:0x1:0x0] orphan.txt'
        )

        mock_popen.return_value = _mock_popen(
            creat_unknown_parent + '\n',
        )
        fid2path_result = MagicMock()
        fid2path_result.stdout = 'some/dir\n'
        fid2path_result.returncode = 0
        mock_run.side_effect = [fid2path_result]
        src = _make_source()
        events = src.read()

        assert len(events) == 1
        assert events[0]['path'] == '/mnt/fs0/some/dir/orphan.txt'
        assert mock_run.call_count == 1
        fid2path_call = mock_run.call_args_list[0]
        assert fid2path_call == call(
            ['sudo', 'lfs', 'fid2path', 'fs0', '0xUNKNOWN:0x1:0x0'],
            capture_output=True,
            text=True,
            check=True,
        )

    @patch('src.icicle.lustre_source.subprocess.run')
    @patch('src.icicle.lustre_source.subprocess.Popen')
    def test_fid2path_result_cached(
        self, mock_popen: MagicMock, mock_run: MagicMock,
    ) -> None:
        """After fid2path resolves a FID, it's cached for next use."""
        creat1 = (
            '1 01CREAT 02:52:19.797 2026.03.16'
            ' 0x0 t=[0x200000401:0xA:0x0] ef=0xf u=1000:1000'
            ' nid=172.31.39.56@tcp p=[0xNEW:0x1:0x0] a.txt'
        )
        creat2 = (
            '2 01CREAT 02:52:19.798 2026.03.16'
            ' 0x0 t=[0x200000401:0xB:0x0] ef=0xf u=1000:1000'
            ' nid=172.31.39.56@tcp p=[0xNEW:0x1:0x0] b.txt'
        )
        mock_popen.return_value = _mock_popen(
            creat1 + '\n' + creat2 + '\n',
        )
        fid2path_result = MagicMock()
        fid2path_result.stdout = 'cached/dir\n'
        fid2path_result.returncode = 0
        mock_run.side_effect = [fid2path_result]
        src = _make_source()
        events = src.read()

        assert len(events) == 2
        assert events[0]['path'] == '/mnt/fs0/cached/dir/a.txt'
        assert events[1]['path'] == '/mnt/fs0/cached/dir/b.txt'
        # fid2path called only ONCE — second lookup uses cache.
        assert mock_run.call_count == 1

    @patch('src.icicle.lustre_source.subprocess.run')
    @patch('src.icicle.lustre_source.subprocess.Popen')
    def test_no_fid2path_when_in_cache(
        self, mock_popen: MagicMock, mock_run: MagicMock,
    ) -> None:
        """When parent FID is already cached, no extra fid2path is called."""
        lines = CREAT_DIR + '\n' + CREAT_FILE + '\n'
        mock_popen.return_value = _mock_popen(lines)
        mock_run.side_effect = [_mock_fid2path('/')]
        src = _make_source()
        events = src.read()

        assert len(events) == 2
        # 1 fid2path for root; child's parent was cached from CREAT.
        assert mock_run.call_count == 1


# ===================================================================
# startrec incrementing
# ===================================================================
class TestStartrec:
    @patch('src.icicle.lustre_source.subprocess.run')
    @patch('src.icicle.lustre_source.subprocess.Popen')
    def test_startrec_advances(
        self, mock_popen: MagicMock, mock_run: MagicMock,
    ) -> None:
        mock_popen.return_value = _mock_popen(CREAT_DIR + '\n')
        mock_run.side_effect = [_mock_fid2path('/')]
        src = _make_source(startrec=0)
        src.read()

        assert src._startrec == 2  # eid=1, so next = 2

    @patch('src.icicle.lustre_source.subprocess.run')
    @patch('src.icicle.lustre_source.subprocess.Popen')
    def test_startrec_advances_multiple(
        self, mock_popen: MagicMock, mock_run: MagicMock,
    ) -> None:
        lines = CREAT_DIR + '\n' + CREAT_FILE + '\n'
        mock_popen.return_value = _mock_popen(lines)
        mock_run.side_effect = [_mock_fid2path('/')]
        src = _make_source(startrec=0)
        src.read()

        assert src._startrec == 3  # last eid=2, next = 3

    @patch('src.icicle.lustre_source.subprocess.Popen')
    def test_startrec_passed_to_lfs(
        self, mock_popen: MagicMock,
    ) -> None:
        mock_popen.return_value = _mock_popen('')
        src = _make_source(startrec=42)
        src._read_changelog()

        cmd = mock_popen.call_args[0][0]
        assert 'sudo lfs changelog fs0-MDT0000 42' in cmd


# ===================================================================
# Empty changelog
# ===================================================================
class TestEmptyChangelog:
    def test_closed_returns_empty(self) -> None:
        src = _make_source()
        src.close()
        events = src.read()

        assert events == []

    @patch('src.icicle.lustre_source.subprocess.run')
    @patch('src.icicle.lustre_source.subprocess.Popen')
    def test_empty_then_events(
        self, mock_popen: MagicMock, mock_run: MagicMock,
    ) -> None:
        """read() polls until events arrive."""
        empty = _mock_popen('')
        with_events = _mock_popen(CREAT_DIR + '\n')
        mock_popen.side_effect = [empty, with_events]
        mock_run.side_effect = [_mock_fid2path('/')]
        src = _make_source()
        events = src.read()

        assert len(events) == 1
        assert events[0]['kind'] == EventKind.CREATED


# ===================================================================
# OPEN events dropped
# ===================================================================
class TestOpenMapped:
    @patch('src.icicle.lustre_source.subprocess.run')
    @patch('src.icicle.lustre_source.subprocess.Popen')
    def test_open_maps_to_modified(
        self, mock_popen: MagicMock, mock_run: MagicMock,
    ) -> None:
        lines = CREAT_DIR + '\n' + CREAT_FILE + '\n' + OPEN_FILE + '\n'
        mock_popen.return_value = _mock_popen(lines)
        mock_run.side_effect = [_mock_fid2path('/')]
        src = _make_source()
        events = src.read()

        # 2 CREATED + 1 MODIFIED (from OPEN).
        assert len(events) == 3
        kinds = [e['kind'] for e in events]
        assert kinds.count(EventKind.CREATED) == 2
        assert kinds.count(EventKind.MODIFIED) == 1


# ===================================================================
# Events have kind=EventKind
# ===================================================================
class TestEventKindMapping:
    @patch('src.icicle.lustre_source.subprocess.run')
    @patch('src.icicle.lustre_source.subprocess.Popen')
    def test_all_events_use_event_kind(
        self, mock_popen: MagicMock, mock_run: MagicMock,
    ) -> None:
        lines = (
            '\n'.join(
                [CREAT_DIR, CREAT_FILE, CLOSE_FILE, RENME_FILE, UNLNK_FILE],
            )
            + '\n'
        )
        mock_popen.return_value = _mock_popen(lines)
        mock_run.side_effect = [_mock_fid2path('/')]
        src = _make_source()
        events = src.read()

        for ev in events:
            assert isinstance(ev['kind'], EventKind), (
                f'Expected EventKind, got {type(ev["kind"])}: {ev["kind"]}'
            )

    @patch('src.icicle.lustre_source.subprocess.run')
    @patch('src.icicle.lustre_source.subprocess.Popen')
    def test_close_maps_to_modified(
        self, mock_popen: MagicMock, mock_run: MagicMock,
    ) -> None:
        lines = CREAT_DIR + '\n' + CREAT_FILE + '\n' + CLOSE_FILE + '\n'
        mock_popen.return_value = _mock_popen(lines)
        mock_run.side_effect = [_mock_fid2path('/')]
        src = _make_source()
        events = src.read()

        close_events = [e for e in events if e['kind'] == EventKind.MODIFIED]
        assert len(close_events) == 1

    @patch('src.icicle.lustre_source.subprocess.run')
    @patch('src.icicle.lustre_source.subprocess.Popen')
    def test_sattr_maps_to_modified(
        self, mock_popen: MagicMock, mock_run: MagicMock,
    ) -> None:
        lines = CREAT_DIR + '\n' + CREAT_FILE + '\n' + SATTR_FILE + '\n'
        mock_popen.return_value = _mock_popen(lines)
        mock_run.side_effect = [_mock_fid2path('/')]
        src = _make_source()
        events = src.read()

        modified = [e for e in events if e['kind'] == EventKind.MODIFIED]
        assert len(modified) == 1


# ===================================================================
# close() behavior
# ===================================================================
class TestClose:
    def test_close_stops_reading(self) -> None:
        src = _make_source()
        src.close()
        events = src.read()

        assert events == []

    @patch('src.icicle.lustre_source.subprocess.run')
    @patch('src.icicle.lustre_source.subprocess.Popen')
    def test_read_after_close(
        self, mock_popen: MagicMock, mock_run: MagicMock,
    ) -> None:
        mock_popen.return_value = _mock_popen(CREAT_DIR + '\n')
        mock_run.side_effect = [_mock_fid2path('/')]
        src = _make_source()
        events = src.read()
        assert len(events) == 1
        src.close()
        assert src.read() == []


# ===================================================================
# Full sequence
# ===================================================================
class TestFullSequence:
    @patch('src.icicle.lustre_source.subprocess.run')
    @patch('src.icicle.lustre_source.subprocess.Popen')
    def test_mkdir_creat_rename_unlnk_rmdir(
        self, mock_popen: MagicMock, mock_run: MagicMock,
    ) -> None:
        """Full lifecycle: mkdir → creat → rename → unlnk → rmdir."""
        unlnk_renamed = (
            '6 06UNLNK 02:52:19.807 2026.03.16'
            ' 0x1 t=[0x200000401:0x3:0x0] ef=0xf u=1000:1000'
            ' nid=172.31.39.56@tcp p=[0x200000401:0x2:0x0] renamed.txt'
        )
        lines = (
            '\n'.join(
                [
                    CREAT_DIR,
                    CREAT_FILE,
                    RENME_FILE,
                    unlnk_renamed,
                    RMDIR_DIR,
                ],
            )
            + '\n'
        )
        mock_popen.return_value = _mock_popen(lines)
        mock_run.side_effect = [_mock_fid2path('/')]
        src = _make_source()
        events = src.read()

        paths_and_kinds = [(e['path'], e['kind']) for e in events]
        assert paths_and_kinds == [
            ('/mnt/fs0/mydir', EventKind.CREATED),
            ('/mnt/fs0/mydir/file1.txt', EventKind.CREATED),
            ('/mnt/fs0/mydir/file1.txt', EventKind.RENAMED),
            ('/mnt/fs0/mydir/renamed.txt', EventKind.RENAMED),
            ('/mnt/fs0/mydir/renamed.txt', EventKind.REMOVED),
            ('/mnt/fs0/mydir', EventKind.REMOVED),
        ]
