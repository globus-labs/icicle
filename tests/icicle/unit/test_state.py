"""Tests for icicle StateManager, especially recursive rename."""

from __future__ import annotations

import pytest

from src.icicle.events import EventKind
from src.icicle.state import StateManager


def ev(path: str, kind: EventKind, is_dir: bool = False, **kw) -> dict:
    return {'path': path, 'kind': kind, 'ts': 0.0, 'is_dir': is_dir, **kw}


def make_stat_result(**kw):
    """Patch os.stat to return a fake stat result."""
    defaults = {
        'st_size': 100,
        'st_uid': 1000,
        'st_gid': 1000,
        'st_mode': 0o100644,
        'st_atime': 1000,
        'st_mtime': 2000,
        'st_ctime': 3000,
    }
    defaults.update(kw)

    class FakeStat:
        pass

    s = FakeStat()
    for k, v in defaults.items():
        setattr(s, k, v)
    return s


@pytest.fixture
def sm():
    return StateManager(stat_fn=lambda p: make_stat_result())


# ---------------------------------------------------------------------------
# Creation
# ---------------------------------------------------------------------------


class TestCreation:
    def test_create_file(self, sm):
        sm.process_events([ev('/tmp/a.txt', EventKind.CREATED)])
        assert '/tmp/a.txt' in sm.files
        assert '/tmp/a.txt' in sm.to_update
        assert sm.files['/tmp/a.txt']['is_dir'] is False
        assert sm.files['/tmp/a.txt']['name'] == 'a.txt'
        assert sm.files['/tmp/a.txt']['parent'] == '/tmp'

    def test_create_dir(self, sm):
        sm.process_events([ev('/tmp/subdir', EventKind.CREATED, is_dir=True)])
        assert '/tmp/subdir' in sm.files
        assert sm.files['/tmp/subdir']['is_dir'] is True
        assert '/tmp/subdir' in sm.children  # dir has children key

    def test_create_updates_parent(self, sm):
        sm.process_events([ev('/tmp', EventKind.CREATED, is_dir=True)])
        sm.process_events([ev('/tmp/a.txt', EventKind.CREATED)])
        assert '/tmp' in sm.to_update  # parent updated
        assert '/tmp/a.txt' in sm.children['/tmp']

    def test_emit_after_create(self, sm):
        sm.process_events([ev('/tmp/a.txt', EventKind.CREATED)])
        changes = sm.emit()
        assert len(changes) == 1
        assert changes[0]['op'] == 'update'
        assert changes[0]['path'] == '/tmp/a.txt'
        assert 'stat' in changes[0]
        assert changes[0]['stat']['size'] == 100

    def test_create_symlink_tracked(self, sm):
        """CREATED with is_symlink=True — entry tracks the flag."""
        sm.process_events(
            [ev('/link.txt', EventKind.CREATED, is_symlink=True)],
        )
        assert sm.files['/link.txt']['is_symlink'] is True
        assert len(sm.emit()) == 1

    def test_symlink_and_target_independent(self, sm):
        """Symlink and target are independent entries."""
        sm.process_events(
            [
                ev('/target.txt', EventKind.CREATED),
                ev('/link.txt', EventKind.CREATED, is_symlink=True),
            ],
        )
        assert not sm.files['/target.txt']['is_symlink']
        assert sm.files['/link.txt']['is_symlink']
        sm.process_events([ev('/link.txt', EventKind.REMOVED)])
        assert '/target.txt' in sm.files
        assert '/link.txt' not in sm.files

    def test_symlink_modify_tracked(self, sm):
        """MODIFIED for symlink path — marked for update."""
        sm.process_events(
            [ev('/link.txt', EventKind.CREATED, is_symlink=True)],
        )
        sm.to_update.clear()
        sm.process_events([ev('/link.txt', EventKind.MODIFIED)])
        assert '/link.txt' in sm.to_update


# ---------------------------------------------------------------------------
# Removal
# ---------------------------------------------------------------------------


class TestRemoval:
    def test_remove_file(self, sm):
        sm.process_events([ev('/tmp/a.txt', EventKind.CREATED)])
        sm.process_events([ev('/tmp/a.txt', EventKind.REMOVED)])
        assert '/tmp/a.txt' not in sm.files
        assert '/tmp/a.txt' in sm.to_delete

    def test_create_and_remove_never_emitted(self, sm):
        """If never emitted, delete doesn't show in output."""
        sm.process_events(
            [
                ev('/tmp/a.txt', EventKind.CREATED),
                ev('/tmp/a.txt', EventKind.REMOVED),
            ],
        )
        changes = sm.emit()
        # Never emitted, so no delete event
        assert len(changes) == 0

    def test_remove_after_emit_produces_delete(self, sm):
        sm.process_events([ev('/tmp/a.txt', EventKind.CREATED)])
        sm.emit()  # now it's been emitted
        sm.process_events([ev('/tmp/a.txt', EventKind.REMOVED)])
        changes = sm.emit()
        assert len(changes) == 1
        assert changes[0]['op'] == 'delete'
        assert changes[0]['path'] == '/tmp/a.txt'

    def test_remove_unknown_path_ignored(self, sm):
        sm.process_events([ev('/tmp/nonexistent', EventKind.REMOVED)])
        # Should not raise, just log debug

    def test_remove_directory_cascades(self, sm):
        sm.process_events(
            [
                ev('/tmp/dir', EventKind.CREATED, is_dir=True),
                ev('/tmp/dir/a.txt', EventKind.CREATED),
                ev('/tmp/dir/b.txt', EventKind.CREATED),
                ev('/tmp/dir/sub', EventKind.CREATED, is_dir=True),
                ev('/tmp/dir/sub/c.txt', EventKind.CREATED),
            ],
        )
        sm.process_events([ev('/tmp/dir', EventKind.REMOVED, is_dir=True)])
        assert '/tmp/dir' not in sm.files
        assert '/tmp/dir/a.txt' not in sm.files
        assert '/tmp/dir/b.txt' not in sm.files
        assert '/tmp/dir/sub' not in sm.files
        assert '/tmp/dir/sub/c.txt' not in sm.files


# ---------------------------------------------------------------------------
# Updates
# ---------------------------------------------------------------------------


class TestUpdates:
    def test_update_known_path(self, sm):
        sm.process_events([ev('/tmp/a.txt', EventKind.CREATED)])
        sm.to_update.clear()  # reset
        sm.process_events([ev('/tmp/a.txt', EventKind.MODIFIED)])
        assert '/tmp/a.txt' in sm.to_update

    def test_update_unknown_path_ignored(self, sm):
        sm.process_events([ev('/tmp/unknown', EventKind.MODIFIED)])
        assert '/tmp/unknown' not in sm.to_update
        assert '/tmp/unknown' not in sm.files


# ---------------------------------------------------------------------------
# Rename (file)
# ---------------------------------------------------------------------------


class TestRenameFile:
    def test_rename_file(self, sm):
        sm.process_events([ev('/tmp/old.txt', EventKind.CREATED)])
        # Two RENAMED events: old path then new path
        sm.process_events(
            [
                ev('/tmp/old.txt', EventKind.RENAMED),
                ev('/tmp/new.txt', EventKind.RENAMED),
            ],
        )
        assert '/tmp/old.txt' not in sm.files
        assert '/tmp/new.txt' in sm.files
        assert sm.files['/tmp/new.txt']['name'] == 'new.txt'

    def test_rename_preserves_entry_data(self, sm):
        sm.process_events(
            [ev('/tmp/old.txt', EventKind.CREATED, is_symlink=True)],
        )
        sm.process_events(
            [
                ev('/tmp/old.txt', EventKind.RENAMED),
                ev('/tmp/new.txt', EventKind.RENAMED),
            ],
        )
        assert sm.files['/tmp/new.txt']['is_symlink'] is True

    def test_rename_updates_parent_children(self, sm):
        sm.process_events(
            [
                ev('/tmp', EventKind.CREATED, is_dir=True),
                ev('/tmp/old.txt', EventKind.CREATED),
            ],
        )
        sm.process_events(
            [
                ev('/tmp/old.txt', EventKind.RENAMED),
                ev('/tmp/new.txt', EventKind.RENAMED),
            ],
        )
        assert '/tmp/old.txt' not in sm.children['/tmp']
        assert '/tmp/new.txt' in sm.children['/tmp']

    def test_rename_tracks_emitted(self, sm):
        sm.process_events([ev('/tmp/a.txt', EventKind.CREATED)])
        sm.emit()  # emitted
        sm.process_events(
            [
                ev('/tmp/a.txt', EventKind.RENAMED),
                ev('/tmp/b.txt', EventKind.RENAMED),
            ],
        )
        changes = sm.emit()
        ops = {c['op'] for c in changes}
        paths = {c['path'] for c in changes}
        assert 'delete' in ops
        assert 'update' in ops
        assert '/tmp/a.txt' in paths  # delete old
        assert '/tmp/b.txt' in paths  # update new

    def test_rename_with_collision(self, sm):
        """Rename to a path that already exists — existing gets deleted."""
        sm.process_events(
            [
                ev('/tmp/a.txt', EventKind.CREATED),
                ev('/tmp/b.txt', EventKind.CREATED),
            ],
        )
        sm.emit()
        sm.process_events(
            [
                ev('/tmp/a.txt', EventKind.RENAMED),
                ev('/tmp/b.txt', EventKind.RENAMED),
            ],
        )
        # b.txt (old) got deleted, a.txt moved to b.txt
        assert '/tmp/a.txt' not in sm.files
        assert '/tmp/b.txt' in sm.files
        changes = sm.emit()
        delete_paths = {c['path'] for c in changes if c['op'] == 'delete'}
        assert '/tmp/a.txt' in delete_paths


# ---------------------------------------------------------------------------
# Rename (directory, recursive)
# ---------------------------------------------------------------------------


class TestRenameDirectory:
    def test_rename_dir_recursive(self, sm):
        """Rename /a/ → /b/ with children: /a/x.txt, /a/sub/, /a/sub/y.txt"""
        sm.process_events(
            [
                ev('/a', EventKind.CREATED, is_dir=True),
                ev('/a/x.txt', EventKind.CREATED),
                ev('/a/sub', EventKind.CREATED, is_dir=True),
                ev('/a/sub/y.txt', EventKind.CREATED),
            ],
        )
        sm.process_events(
            [
                ev('/a', EventKind.RENAMED, is_dir=True),
                ev('/b', EventKind.RENAMED, is_dir=True),
            ],
        )
        # Old paths gone
        assert '/a' not in sm.files
        assert '/a/x.txt' not in sm.files
        assert '/a/sub' not in sm.files
        assert '/a/sub/y.txt' not in sm.files
        # New paths exist
        assert '/b' in sm.files
        assert '/b/x.txt' in sm.files
        assert '/b/sub' in sm.files
        assert '/b/sub/y.txt' in sm.files
        # All new paths in to_update
        assert '/b' in sm.to_update
        assert '/b/x.txt' in sm.to_update
        assert '/b/sub' in sm.to_update
        assert '/b/sub/y.txt' in sm.to_update

    def test_rename_dir_children_structure(self, sm):
        """Verify children dict is updated correctly after dir rename."""
        sm.process_events(
            [
                ev('/a', EventKind.CREATED, is_dir=True),
                ev('/a/x.txt', EventKind.CREATED),
                ev('/a/sub', EventKind.CREATED, is_dir=True),
                ev('/a/sub/y.txt', EventKind.CREATED),
            ],
        )
        sm.process_events(
            [
                ev('/a', EventKind.RENAMED, is_dir=True),
                ev('/b', EventKind.RENAMED, is_dir=True),
            ],
        )
        assert '/b/x.txt' in sm.children['/b']
        assert '/b/sub' in sm.children['/b']
        assert '/b/sub/y.txt' in sm.children['/b/sub']
        # Old children entries gone
        assert '/a' not in sm.children
        assert '/a/sub' not in sm.children

    def test_rename_nested_dir(self, sm):
        """Rename /a/sub/ → /a/newsub/"""
        sm.process_events(
            [
                ev('/a', EventKind.CREATED, is_dir=True),
                ev('/a/sub', EventKind.CREATED, is_dir=True),
                ev('/a/sub/y.txt', EventKind.CREATED),
            ],
        )
        sm.process_events(
            [
                ev('/a/sub', EventKind.RENAMED, is_dir=True),
                ev('/a/newsub', EventKind.RENAMED, is_dir=True),
            ],
        )
        assert '/a/sub' not in sm.files
        assert '/a/sub/y.txt' not in sm.files
        assert '/a/newsub' in sm.files
        assert '/a/newsub/y.txt' in sm.files

    def test_rename_deep_tree(self, sm):
        """3-level deep rename: /a/b/c/ with file at each level."""
        sm.process_events(
            [
                ev('/root', EventKind.CREATED, is_dir=True),
                ev('/root/a', EventKind.CREATED, is_dir=True),
                ev('/root/a/f1.txt', EventKind.CREATED),
                ev('/root/a/b', EventKind.CREATED, is_dir=True),
                ev('/root/a/b/f2.txt', EventKind.CREATED),
                ev('/root/a/b/c', EventKind.CREATED, is_dir=True),
                ev('/root/a/b/c/f3.txt', EventKind.CREATED),
            ],
        )
        sm.process_events(
            [
                ev('/root/a', EventKind.RENAMED, is_dir=True),
                ev('/root/z', EventKind.RENAMED, is_dir=True),
            ],
        )
        assert '/root/z' in sm.files
        assert '/root/z/f1.txt' in sm.files
        assert '/root/z/b' in sm.files
        assert '/root/z/b/f2.txt' in sm.files
        assert '/root/z/b/c' in sm.files
        assert '/root/z/b/c/f3.txt' in sm.files

    def test_rename_dir_emitted_produces_deletes(self, sm):
        """After emit, renaming a dir should produce delete events for old paths."""
        sm.process_events(
            [
                ev('/a', EventKind.CREATED, is_dir=True),
                ev('/a/x.txt', EventKind.CREATED),
            ],
        )
        sm.emit()  # now emitted
        sm.process_events(
            [
                ev('/a', EventKind.RENAMED, is_dir=True),
                ev('/b', EventKind.RENAMED, is_dir=True),
            ],
        )
        changes = sm.emit()
        delete_paths = {c['path'] for c in changes if c['op'] == 'delete'}
        update_paths = {c['path'] for c in changes if c['op'] == 'update'}
        assert '/a' in delete_paths
        assert '/a/x.txt' in delete_paths
        assert '/b' in update_paths
        assert '/b/x.txt' in update_paths


# ---------------------------------------------------------------------------
# Rename edge cases
# ---------------------------------------------------------------------------


class TestRenameEdgeCases:
    def test_unpaired_rename_treated_as_removal(self, sm):
        """Single rename event followed by non-rename → treated as removal."""
        sm.process_events([ev('/tmp/a.txt', EventKind.CREATED)])
        sm.process_events([ev('/tmp/a.txt', EventKind.RENAMED)])
        # Now process a non-rename event to flush the pending rename
        sm.process_events([ev('/tmp/b.txt', EventKind.CREATED)])
        assert '/tmp/a.txt' not in sm.files  # removed

    def test_unpaired_rename_flushed_after_grace(self, sm):
        """Pending rename gets one grace emit(), flushed on the second."""
        sm.process_events([ev('/tmp/a.txt', EventKind.CREATED)])
        sm.emit()
        sm.process_events([ev('/tmp/a.txt', EventKind.RENAMED)])
        changes1 = sm.emit()  # grace cycle
        assert not any(
            c['op'] == 'delete' and c['path'] == '/tmp/a.txt' for c in changes1
        )
        changes2 = sm.emit()  # flushed
        assert any(
            c['op'] == 'delete' and c['path'] == '/tmp/a.txt' for c in changes2
        )

    def test_rename_unknown_path_creates_new(self, sm):
        """Rename pair where old path is unknown → treated as creation."""
        sm.process_events(
            [
                ev('/tmp/unknown', EventKind.RENAMED),
                ev('/tmp/new', EventKind.RENAMED),
            ],
        )
        assert '/tmp/new' in sm.files


# ---------------------------------------------------------------------------
# Emit behavior
# ---------------------------------------------------------------------------


class TestEmit:
    def test_emit_clears_pending(self, sm):
        sm.process_events([ev('/tmp/a.txt', EventKind.CREATED)])
        sm.emit()
        assert len(sm.to_update) == 0
        assert len(sm.to_delete) == 0

    def test_emit_format(self, sm):
        sm.process_events([ev('/tmp/a.txt', EventKind.CREATED)])
        changes = sm.emit()
        assert len(changes) == 1
        c = changes[0]
        assert c['op'] == 'update'
        assert c['path'] == '/tmp/a.txt'
        assert 'stat' in c
        stat = c['stat']
        assert 'size' in stat
        assert 'uid' in stat
        assert 'gid' in stat
        assert 'mode' in stat
        assert 'atime' in stat
        assert 'mtime' in stat
        assert 'ctime' in stat

    def test_emit_stat_failure_skips_event(self):
        """If os.stat fails (file gone), emit skips the event."""

        def _fail(p):
            raise OSError('gone')

        sm = StateManager(stat_fn=_fail)
        sm.process_events([ev('/tmp/nonexistent_file', EventKind.CREATED)])
        changes = sm.emit()
        assert len(changes) == 0

    def test_double_emit_no_duplicates(self, sm):
        sm.process_events([ev('/tmp/a.txt', EventKind.CREATED)])
        sm.emit()
        changes = sm.emit()
        assert len(changes) == 0  # nothing new to emit

    def test_update_exact_keys(self, sm):
        """Update has exactly {op, path, stat} with 7 stat sub-keys."""
        sm.process_events([ev('/f.txt', EventKind.CREATED)])
        c = sm.emit()[0]
        assert set(c.keys()) == {'op', 'fid', 'path', 'stat'}
        assert set(c['stat'].keys()) == {
            'size',
            'uid',
            'gid',
            'mode',
            'atime',
            'mtime',
            'ctime',
        }

    def test_delete_no_stat_key(self, sm):
        """Delete has exactly {op, path} — no stat."""
        sm.process_events([ev('/f.txt', EventKind.CREATED)])
        sm.emit()
        sm.process_events([ev('/f.txt', EventKind.REMOVED)])
        c = sm.emit()[0]
        assert set(c.keys()) == {'op', 'fid', 'path'}

    def test_process_events_empty(self, sm):
        """process_events([]) is a no-op."""
        sm.process_events([ev('/a.txt', EventKind.CREATED)])
        sm.process_events([])
        assert len(sm.files) == 1
