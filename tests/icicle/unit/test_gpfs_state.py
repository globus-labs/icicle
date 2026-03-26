"""Tests for GPFSStateManager — inode-keyed state management."""

from __future__ import annotations

import pytest

from src.icicle.events import EventKind
from src.icicle.gpfs_state import GPFSStateManager


def ev(
    inode: int,
    path: str,
    kind: EventKind,
    is_dir: bool = False,
    **kw,
) -> dict:
    """Helper to create GPFS event dicts."""
    return {
        'inode': inode,
        'path': path,
        'kind': kind,
        'is_dir': is_dir,
        'size': kw.get('size'),
        'atime': kw.get('atime'),
        'ctime': kw.get('ctime'),
        'mtime': kw.get('mtime'),
        'uid': kw.get('uid'),
        'gid': kw.get('gid'),
        'permissions': kw.get('permissions'),
        **{
            k: v
            for k, v in kw.items()
            if k
            not in (
                'size',
                'atime',
                'ctime',
                'mtime',
                'uid',
                'gid',
                'permissions',
            )
        },
    }


@pytest.fixture
def sm():
    return GPFSStateManager()


# ---------------------------------------------------------------------------
# Creation
# ---------------------------------------------------------------------------


class TestCreation:
    def test_create_file(self, sm):
        sm.process_events(
            [ev(100, '/gpfs/file.txt', EventKind.CREATED, size=1024)],
        )
        assert '100' in sm.files
        assert sm.files['100']['path'] == '/gpfs/file.txt'
        assert sm.files['100']['size'] == 1024
        assert '100' in sm.to_update

    def test_create_dir(self, sm):
        sm.process_events(
            [ev(200, '/gpfs/subdir', EventKind.CREATED, is_dir=True)],
        )
        assert '200' in sm.files
        assert sm.files['200']['is_dir'] is True
        assert '200' in sm.children

    def test_create_updates_parent(self, sm):
        sm.process_events([ev(10, '/gpfs', EventKind.CREATED, is_dir=True)])
        sm.process_events([ev(100, '/gpfs/file.txt', EventKind.CREATED)])
        assert '100' in sm.children['10']
        assert '10' in sm.to_update  # parent marked for update

    def test_create_populates_path_to_fid(self, sm):
        sm.process_events([ev(100, '/gpfs/file.txt', EventKind.CREATED)])
        assert sm.path_to_fid['/gpfs/file.txt'] == '100'

    def test_emit_after_create(self, sm):
        sm.process_events(
            [
                ev(
                    100,
                    '/gpfs/file.txt',
                    EventKind.CREATED,
                    size=42,
                    mtime=1000,
                    uid=500,
                    gid=500,
                    permissions='0644',
                ),
            ],
        )
        changes = sm.emit()
        assert len(changes) == 1
        assert changes[0]['op'] == 'update'
        assert changes[0]['fid'] == '100'
        assert changes[0]['path'] == '/gpfs/file.txt'
        assert changes[0]['stat']['size'] == 42
        assert changes[0]['stat']['uid'] == 500


# ---------------------------------------------------------------------------
# Deletion
# ---------------------------------------------------------------------------


class TestDeletion:
    def test_delete_tracked_file(self, sm):
        sm.process_events([ev(100, '/gpfs/file.txt', EventKind.CREATED)])
        sm.emit()  # must emit first so delete is reported
        sm.process_events([ev(100, '/gpfs/file.txt', EventKind.REMOVED)])
        changes = sm.emit()
        deletes = [c for c in changes if c['op'] == 'delete']
        assert len(deletes) == 1
        assert deletes[0]['fid'] == '100'

    def test_delete_untracked_file_no_output(self, sm):
        sm.process_events([ev(999, '/gpfs/unknown.txt', EventKind.REMOVED)])
        changes = sm.emit()
        assert len(changes) == 0

    def test_delete_never_emitted_no_delete_output(self, sm):
        sm.process_events([ev(100, '/gpfs/file.txt', EventKind.CREATED)])
        # Don't emit — file was never reported to output
        sm.process_events([ev(100, '/gpfs/file.txt', EventKind.REMOVED)])
        changes = sm.emit()
        # No output at all — transient file
        assert len(changes) == 0

    def test_delete_updates_parent(self, sm):
        sm.process_events([ev(10, '/gpfs', EventKind.CREATED, is_dir=True)])
        sm.process_events([ev(100, '/gpfs/file.txt', EventKind.CREATED)])
        sm.to_update.clear()
        sm.process_events([ev(100, '/gpfs/file.txt', EventKind.REMOVED)])
        assert '10' in sm.to_update

    def test_delete_removes_from_files(self, sm):
        sm.process_events([ev(100, '/gpfs/file.txt', EventKind.CREATED)])
        sm.process_events([ev(100, '/gpfs/file.txt', EventKind.REMOVED)])
        assert '100' not in sm.files


# ---------------------------------------------------------------------------
# Update (MODIFIED / ACCESSED)
# ---------------------------------------------------------------------------


class TestUpdate:
    def test_modify_updates_metadata(self, sm):
        sm.process_events(
            [
                ev(
                    100,
                    '/gpfs/file.txt',
                    EventKind.CREATED,
                    size=100,
                    mtime=1000,
                ),
            ],
        )
        sm.process_events(
            [
                ev(
                    100,
                    '/gpfs/file.txt',
                    EventKind.MODIFIED,
                    size=200,
                    mtime=2000,
                ),
            ],
        )
        assert sm.files['100']['size'] == 200
        assert sm.files['100']['mtime'] == 2000

    def test_modify_unknown_fid_ignored(self, sm):
        sm.process_events(
            [
                ev(999, '/gpfs/unknown.txt', EventKind.MODIFIED, size=100),
            ],
        )
        assert '999' not in sm.files

    def test_multiple_modifies_keep_latest(self, sm):
        sm.process_events(
            [
                ev(100, '/gpfs/file.txt', EventKind.CREATED, size=10),
                ev(100, '/gpfs/file.txt', EventKind.MODIFIED, size=20),
                ev(100, '/gpfs/file.txt', EventKind.MODIFIED, size=30),
            ],
        )
        changes = sm.emit()
        assert len(changes) == 1
        assert changes[0]['stat']['size'] == 30


# ---------------------------------------------------------------------------
# Rename (single-event via IN_MOVED_TO)
# ---------------------------------------------------------------------------


class TestRename:
    def test_simple_rename(self, sm):
        sm.process_events([ev(100, '/gpfs/old.txt', EventKind.CREATED)])
        sm.process_events([ev(100, '/gpfs/new.txt', EventKind.RENAMED)])
        assert sm.files['100']['path'] == '/gpfs/new.txt'
        assert sm.path_to_fid.get('/gpfs/old.txt') is None
        assert sm.path_to_fid['/gpfs/new.txt'] == '100'

    def test_rename_emits_update(self, sm):
        sm.process_events(
            [
                ev(100, '/gpfs/old.txt', EventKind.CREATED, size=50),
            ],
        )
        sm.process_events(
            [
                ev(100, '/gpfs/new.txt', EventKind.RENAMED, size=50),
            ],
        )
        changes = sm.emit()
        updates = [c for c in changes if c['op'] == 'update']
        assert any(c['path'] == '/gpfs/new.txt' for c in updates)

    def test_rename_emits_delete_for_old_path(self, sm):
        sm.process_events([ev(100, '/gpfs/old.txt', EventKind.CREATED)])
        sm.emit()  # emit old path first
        sm.process_events([ev(100, '/gpfs/new.txt', EventKind.RENAMED)])
        changes = sm.emit()
        # Old path should NOT get a delete — it's the same inode,
        # GPFS state is inode-keyed. The old path just updates to new path.
        paths = {c['path'] for c in changes}
        assert '/gpfs/new.txt' in paths

    def test_rename_unknown_fid_ignored(self, sm):
        sm.process_events([ev(999, '/gpfs/new.txt', EventKind.RENAMED)])
        assert '999' not in sm.files


# ---------------------------------------------------------------------------
# Rename collision detection
# ---------------------------------------------------------------------------


class TestRenameCollision:
    def test_collision_synthesizes_deletion(self, sm):
        sm.process_events(
            [
                ev(100, '/gpfs/a.txt', EventKind.CREATED),
                ev(200, '/gpfs/b.txt', EventKind.CREATED),
            ],
        )
        sm.emit()  # emit both
        # Rename inode 100 to /gpfs/b.txt — collides with inode 200
        sm.process_events([ev(100, '/gpfs/b.txt', EventKind.RENAMED)])
        assert '200' not in sm.files  # collision victim removed
        assert sm.files['100']['path'] == '/gpfs/b.txt'
        changes = sm.emit()
        delete_fids = {c['fid'] for c in changes if c['op'] == 'delete'}
        assert '200' in delete_fids


# ---------------------------------------------------------------------------
# Directory rename with recursive child path update
# ---------------------------------------------------------------------------


class TestDirectoryRename:
    def test_recursive_child_update(self, sm):
        # Create parent dir and children
        sm.process_events(
            [
                ev(10, '/gpfs/adir', EventKind.CREATED, is_dir=True),
            ],
        )
        sm.process_events(
            [
                ev(100, '/gpfs/adir/file.txt', EventKind.CREATED),
                ev(20, '/gpfs/adir/sub', EventKind.CREATED, is_dir=True),
            ],
        )
        sm.process_events(
            [
                ev(200, '/gpfs/adir/sub/deep.txt', EventKind.CREATED),
            ],
        )

        # Rename /gpfs/adir -> /gpfs/bdir
        sm.process_events([ev(10, '/gpfs/bdir', EventKind.RENAMED)])

        assert sm.files['10']['path'] == '/gpfs/bdir'
        assert sm.files['100']['path'] == '/gpfs/bdir/file.txt'
        assert sm.files['20']['path'] == '/gpfs/bdir/sub'
        assert sm.files['200']['path'] == '/gpfs/bdir/sub/deep.txt'

        # Path index updated
        assert sm.path_to_fid.get('/gpfs/adir') is None
        assert sm.path_to_fid['/gpfs/bdir'] == '10'
        assert sm.path_to_fid['/gpfs/bdir/file.txt'] == '100'

    def test_cross_directory_move(self, sm):
        # Setup: /gpfs/src/file.txt and /gpfs/dst/
        sm.process_events(
            [
                ev(10, '/gpfs/src', EventKind.CREATED, is_dir=True),
                ev(20, '/gpfs/dst', EventKind.CREATED, is_dir=True),
            ],
        )
        sm.process_events(
            [
                ev(100, '/gpfs/src/file.txt', EventKind.CREATED),
            ],
        )

        # Move: /gpfs/src/file.txt -> /gpfs/dst/file.txt
        sm.process_events([ev(100, '/gpfs/dst/file.txt', EventKind.RENAMED)])

        assert sm.files['100']['path'] == '/gpfs/dst/file.txt'
        assert sm.path_to_fid['/gpfs/dst/file.txt'] == '100'
        # File moved out of src, into dst
        assert '100' not in sm.children.get('10', set())
        assert '100' in sm.children['20']


# ---------------------------------------------------------------------------
# Emit format
# ---------------------------------------------------------------------------


class TestEmitFormat:
    def test_update_has_fid_and_stat(self, sm):
        sm.process_events(
            [
                ev(
                    100,
                    '/gpfs/f.txt',
                    EventKind.CREATED,
                    size=42,
                    atime=1,
                    ctime=2,
                    mtime=3,
                    uid=500,
                    gid=500,
                    permissions='0644',
                ),
            ],
        )
        changes = sm.emit()
        assert len(changes) == 1
        c = changes[0]
        assert c['op'] == 'update'
        assert c['fid'] == '100'
        assert c['path'] == '/gpfs/f.txt'
        assert c['stat']['size'] == 42
        assert c['stat']['mode'] == '0644'
        assert c['stat']['uid'] == 500

    def test_delete_has_fid(self, sm):
        sm.process_events([ev(100, '/gpfs/f.txt', EventKind.CREATED)])
        sm.emit()
        sm.process_events([ev(100, '/gpfs/f.txt', EventKind.REMOVED)])
        changes = sm.emit()
        deletes = [c for c in changes if c['op'] == 'delete']
        assert len(deletes) == 1
        assert deletes[0]['fid'] == '100'

    def test_emit_clears_pending(self, sm):
        sm.process_events([ev(100, '/gpfs/f.txt', EventKind.CREATED)])
        sm.emit()
        changes = sm.emit()
        assert len(changes) == 0


# ---------------------------------------------------------------------------
# Collision edge cases
# ---------------------------------------------------------------------------


class TestCollisionEdgeCases:
    def test_collision_victim_already_deleted(self, sm):
        """path_to_fid points to a fid no longer in files."""
        sm.process_events(
            [
                ev(100, '/gpfs/a.txt', EventKind.CREATED),
                ev(200, '/gpfs/b.txt', EventKind.CREATED),
            ],
        )
        sm.files.pop('200')
        sm.process_events([ev(100, '/gpfs/b.txt', EventKind.RENAMED)])
        assert sm.files['100']['path'] == '/gpfs/b.txt'
        assert sm.path_to_fid['/gpfs/b.txt'] == '100'

    def test_collision_victim_is_directory_with_children(self, sm):
        """Rename collides with an existing directory that has children."""
        sm.process_events(
            [
                ev(10, '/gpfs/target', EventKind.CREATED, is_dir=True),
                ev(100, '/gpfs/target/child.txt', EventKind.CREATED),
                ev(200, '/gpfs/source.txt', EventKind.CREATED),
            ],
        )
        sm.emit()
        sm.process_events([ev(200, '/gpfs/target', EventKind.RENAMED)])
        assert '10' not in sm.files
        assert sm.files['200']['path'] == '/gpfs/target'
        changes = sm.emit()
        delete_fids = {c['fid'] for c in changes if c['op'] == 'delete'}
        assert '10' in delete_fids

    def test_cascading_collisions(self, sm):
        """Three files at same path via rapid renames."""
        sm.process_events(
            [
                ev(100, '/gpfs/slot.txt', EventKind.CREATED),
                ev(200, '/gpfs/other1.txt', EventKind.CREATED),
                ev(300, '/gpfs/other2.txt', EventKind.CREATED),
            ],
        )
        sm.emit()
        sm.process_events([ev(200, '/gpfs/slot.txt', EventKind.RENAMED)])
        assert '100' not in sm.files
        sm.process_events([ev(300, '/gpfs/slot.txt', EventKind.RENAMED)])
        assert '200' not in sm.files
        assert sm.files['300']['path'] == '/gpfs/slot.txt'
        assert sm.path_to_fid['/gpfs/slot.txt'] == '300'

    def test_rename_to_self_path(self, sm):
        """Rename where new_path equals current path."""
        sm.process_events(
            [ev(100, '/gpfs/file.txt', EventKind.CREATED, size=10)],
        )
        sm.process_events(
            [ev(100, '/gpfs/file.txt', EventKind.RENAMED, size=20)],
        )
        assert '100' in sm.files
        assert sm.files['100']['path'] == '/gpfs/file.txt'
        assert sm.files['100']['size'] == 20


# ---------------------------------------------------------------------------
# Recursive path update edge cases
# ---------------------------------------------------------------------------


class TestRecursiveUpdateEdgeCases:
    def test_orphaned_child_fid_in_children_set(self, sm):
        """Child fid exists in children set but not in files."""
        sm.process_events(
            [
                ev(10, '/gpfs/dir', EventKind.CREATED, is_dir=True),
                ev(100, '/gpfs/dir/file.txt', EventKind.CREATED),
            ],
        )
        sm.files.pop('100')
        sm.process_events([ev(10, '/gpfs/newdir', EventKind.RENAMED)])
        assert sm.files['10']['path'] == '/gpfs/newdir'

    def test_child_with_no_name(self, sm):
        """Child entry has name=None — should be skipped."""
        sm.process_events(
            [
                ev(10, '/gpfs/dir', EventKind.CREATED, is_dir=True),
                ev(100, '/gpfs/dir/file.txt', EventKind.CREATED),
            ],
        )
        sm.files['100']['name'] = None
        sm.process_events([ev(10, '/gpfs/newdir', EventKind.RENAMED)])
        assert sm.files['10']['path'] == '/gpfs/newdir'
        assert sm.files['100']['path'] == '/gpfs/dir/file.txt'

    def test_deep_nesting_50_levels(self, sm):
        """50-level deep directory tree rename."""
        for i in range(50):
            parent = '/gpfs' + '/d' * i
            path = parent + '/d'
            sm.process_events(
                [ev(i + 1, path, EventKind.CREATED, is_dir=True)],
            )
        deepest = '/gpfs' + '/d' * 50
        sm.process_events([ev(999, deepest + '/leaf.txt', EventKind.CREATED)])
        sm.process_events([ev(1, '/gpfs/renamed', EventKind.RENAMED)])
        expected_leaf = '/gpfs/renamed' + '/d' * 49 + '/leaf.txt'
        assert sm.files['999']['path'] == expected_leaf
        assert sm.path_to_fid[expected_leaf] == '999'

    def test_rename_dir_with_no_children(self, sm):
        """Empty directory rename."""
        sm.process_events(
            [ev(10, '/gpfs/empty', EventKind.CREATED, is_dir=True)],
        )
        sm.process_events([ev(10, '/gpfs/moved', EventKind.RENAMED)])
        assert sm.files['10']['path'] == '/gpfs/moved'
        assert sm.children.get('10') == set()


# ---------------------------------------------------------------------------
# Create/delete ordering edge cases
# ---------------------------------------------------------------------------


class TestOrderingEdgeCases:
    def test_create_child_before_parent(self, sm):
        """Child created when parent not yet in state."""
        sm.process_events([ev(100, '/gpfs/dir/file.txt', EventKind.CREATED)])
        assert sm.files['100']['parent_fid'] is None

    def test_delete_root_level_file(self, sm):
        """File at root level — parent_fid is None."""
        sm.process_events([ev(100, '/file.txt', EventKind.CREATED)])
        sm.emit()
        sm.process_events([ev(100, '/file.txt', EventKind.REMOVED)])
        changes = sm.emit()
        deletes = [c for c in changes if c['op'] == 'delete']
        assert len(deletes) == 1

    def test_inode_reuse_after_delete(self, sm):
        """Same inode used for a new file after deletion."""
        sm.process_events(
            [ev(100, '/gpfs/old.txt', EventKind.CREATED, size=10)],
        )
        sm.emit()
        sm.process_events([ev(100, '/gpfs/old.txt', EventKind.REMOVED)])
        sm.emit()
        sm.process_events(
            [ev(100, '/gpfs/new.txt', EventKind.CREATED, size=20)],
        )
        changes = sm.emit()
        assert len(changes) == 1
        assert changes[0]['path'] == '/gpfs/new.txt'
        assert changes[0]['stat']['size'] == 20

    def test_modify_then_delete_same_batch(self, sm):
        """MODIFIED and REMOVED in same process_events call."""
        sm.process_events([ev(100, '/gpfs/f.txt', EventKind.CREATED)])
        sm.emit()
        sm.process_events(
            [
                ev(100, '/gpfs/f.txt', EventKind.MODIFIED, size=999),
                ev(100, '/gpfs/f.txt', EventKind.REMOVED),
            ],
        )
        changes = sm.emit()
        ops = {c['op'] for c in changes}
        assert 'delete' in ops
        assert 'update' not in ops

    def test_missing_inode_in_all_handlers(self, sm):
        """Events with inode=None should be silently ignored."""
        sm.process_events(
            [
                ev(None, '/gpfs/a', EventKind.CREATED),
                ev(None, '/gpfs/b', EventKind.REMOVED),
                ev(None, '/gpfs/c', EventKind.MODIFIED),
                ev(None, '/gpfs/d', EventKind.RENAMED),
            ],
        )
        assert len(sm.files) == 0

    def test_missing_path_in_create_and_rename(self, sm):
        """Events with empty path should be ignored."""
        sm.process_events([ev(100, '', EventKind.CREATED)])
        assert '100' not in sm.files
        sm.process_events([ev(100, '/gpfs/f.txt', EventKind.CREATED)])
        sm.process_events([ev(100, '', EventKind.RENAMED)])
        assert sm.files['100']['path'] == '/gpfs/f.txt'


# ---------------------------------------------------------------------------
# Emit edge cases
# ---------------------------------------------------------------------------


class TestEmitEdgeCases:
    def test_emit_skips_entry_with_none_path(self, sm):
        """File in to_update but path is None."""
        sm.process_events([ev(100, '/gpfs/f.txt', EventKind.CREATED)])
        sm.files['100']['path'] = None
        changes = sm.emit()
        assert len(changes) == 0

    def test_emit_with_no_events(self, sm):
        assert sm.emit() == []

    def test_emit_multiple_updates_and_deletes(self, sm):
        for i in range(10):
            sm.process_events(
                [ev(i, f'/gpfs/f{i}.txt', EventKind.CREATED, size=i)],
            )
        sm.emit()
        for i in range(10):
            if i % 2 == 0:
                sm.process_events(
                    [
                        ev(
                            i,
                            f'/gpfs/f{i}.txt',
                            EventKind.MODIFIED,
                            size=i * 10,
                        ),
                    ],
                )
            else:
                sm.process_events(
                    [ev(i, f'/gpfs/f{i}.txt', EventKind.REMOVED)],
                )
        changes = sm.emit()
        updates = [c for c in changes if c['op'] == 'update']
        deletes = [c for c in changes if c['op'] == 'delete']
        assert len(updates) == 5
        assert len(deletes) == 5

    def test_to_delete_wins_over_to_update(self, sm):
        """If fid is in both to_update and to_delete, emit delete not update."""
        sm.process_events([ev(100, '/gpfs/f.txt', EventKind.CREATED)])
        sm.emit()
        sm.to_update.add('100')
        sm._delete_paths['100'] = '/gpfs/f.txt'
        sm.to_delete.add('100')
        changes = sm.emit()
        ops = [c['op'] for c in changes if c['fid'] == '100']
        assert ops == ['delete']


# ---------------------------------------------------------------------------
# State consistency
# ---------------------------------------------------------------------------


class TestStateConsistencyGPFS:
    def test_path_to_fid_consistent_after_operations(self, sm):
        sm.process_events(
            [
                ev(10, '/gpfs/dir', EventKind.CREATED, is_dir=True),
                ev(100, '/gpfs/dir/a.txt', EventKind.CREATED),
                ev(200, '/gpfs/dir/b.txt', EventKind.CREATED),
            ],
        )
        sm.process_events([ev(100, '/gpfs/dir/c.txt', EventKind.RENAMED)])
        sm.process_events([ev(200, '/gpfs/dir/b.txt', EventKind.REMOVED)])
        for fid, entry in sm.files.items():
            path = entry.get('path')
            if path:
                assert sm.path_to_fid.get(path) == fid
        for path, fid in sm.path_to_fid.items():
            assert fid in sm.files

    def test_no_stale_paths_after_many_renames(self, sm):
        sm.process_events([ev(100, '/gpfs/v0.txt', EventKind.CREATED)])
        for i in range(1, 20):
            sm.process_events([ev(100, f'/gpfs/v{i}.txt', EventKind.RENAMED)])
        assert sm.path_to_fid.get('/gpfs/v19.txt') == '100'
        for i in range(19):
            assert f'/gpfs/v{i}.txt' not in sm.path_to_fid
