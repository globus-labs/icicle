"""Edge case tests for icicle StateManager."""

from __future__ import annotations

import pytest

from src.icicle.batch import BatchProcessor
from src.icicle.events import EventKind
from src.icicle.fswatch_events import STAT_REDUCTION_RULES
from src.icicle.state import StateManager


def ev(path: str, kind: EventKind, is_dir: bool = False, **kw) -> dict:
    return {'path': path, 'kind': kind, 'ts': 0.0, 'is_dir': is_dir, **kw}


def make_stat_result(**kw):
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
# Create + Update + Emit
# ---------------------------------------------------------------------------


class TestCreateUpdateEmit:
    def test_create_then_update_single_emission(self, sm):
        sm.process_events(
            [
                ev('/tmp/a.txt', EventKind.CREATED),
                ev('/tmp/a.txt', EventKind.MODIFIED),
            ],
        )
        changes = sm.emit()
        # to_update is a set, so only one update emitted
        ups = [c for c in changes if c['op'] == 'update']
        assert len(ups) == 1
        assert ups[0]['path'] == '/tmp/a.txt'

    def test_update_changes_timestamp(self, sm):
        sm.process_events([ev('/tmp/a.txt', EventKind.CREATED, ts=100.0)])
        assert sm.files['/tmp/a.txt']['ts'] == 100.0

        sm.process_events([ev('/tmp/a.txt', EventKind.MODIFIED, ts=200.0)])
        assert sm.files['/tmp/a.txt']['ts'] == 200.0


# ---------------------------------------------------------------------------
# Rename chains
# ---------------------------------------------------------------------------


class TestRenameChains:
    def test_rename_chain_a_to_b_to_c(self, sm):
        sm.process_events([ev('/tmp/a.txt', EventKind.CREATED)])
        # Rename A -> B
        sm.process_events(
            [
                ev('/tmp/a.txt', EventKind.RENAMED),
                ev('/tmp/b.txt', EventKind.RENAMED),
            ],
        )
        assert '/tmp/a.txt' not in sm.files
        assert '/tmp/b.txt' in sm.files

        # Rename B -> C
        sm.process_events(
            [
                ev('/tmp/b.txt', EventKind.RENAMED),
                ev('/tmp/c.txt', EventKind.RENAMED),
            ],
        )
        assert '/tmp/b.txt' not in sm.files
        assert '/tmp/c.txt' in sm.files

    def test_rename_to_same_path(self, sm):
        sm.process_events([ev('/tmp/a.txt', EventKind.CREATED)])
        # Rename A -> A (no-op)
        sm.process_events(
            [
                ev('/tmp/a.txt', EventKind.RENAMED),
                ev('/tmp/a.txt', EventKind.RENAMED),
            ],
        )
        assert '/tmp/a.txt' in sm.files


# ---------------------------------------------------------------------------
# Rename collision with emitted targets
# ---------------------------------------------------------------------------


class TestRenameCollisionEmitted:
    def test_collision_target_was_emitted(self, sm):
        sm.process_events(
            [
                ev('/tmp/a.txt', EventKind.CREATED),
                ev('/tmp/b.txt', EventKind.CREATED),
            ],
        )
        # Emit both (marks them as emitted)
        changes1 = sm.emit()
        assert len(changes1) == 2

        # Rename A -> B (overwrites B)
        sm.process_events(
            [
                ev('/tmp/a.txt', EventKind.RENAMED),
                ev('/tmp/b.txt', EventKind.RENAMED),
            ],
        )
        changes2 = sm.emit()
        # Should have delete for /tmp/a.txt (was emitted, now gone)
        dels = [c for c in changes2 if c['op'] == 'delete']
        assert any(d['path'] == '/tmp/a.txt' for d in dels)


# ---------------------------------------------------------------------------
# Parent-child ordering
# ---------------------------------------------------------------------------


class TestRenameDisplacementNotification:
    """macOS emits a 3rd RENAMED event for the displaced file at the
    target path after an overwriting rename.  The state manager must
    recognise it as a displacement notification and NOT remove the
    entry that was just renamed into that path.
    """

    def test_third_renamed_not_removed(self, sm):
        """CREATED A + CREATED B + RENAMED A + RENAMED B (pair) +
        RENAMED B (displacement) — B must survive.
        """
        sm.process_events(
            [
                ev('/tmp/a.txt', EventKind.CREATED),
                ev('/tmp/b.txt', EventKind.CREATED),
            ],
        )
        sm.emit()

        # Rename pair: A -> B
        sm.process_events(
            [
                ev('/tmp/a.txt', EventKind.RENAMED),
                ev('/tmp/b.txt', EventKind.RENAMED),
            ],
        )
        # Third event: displacement notification for B
        sm.process_events(
            [
                ev('/tmp/b.txt', EventKind.RENAMED),
            ],
        )
        changes = sm.emit()
        updates = [c for c in changes if c['op'] == 'update']
        assert any(u['path'] == '/tmp/b.txt' for u in updates)

    def test_two_displacement_events(self, sm):
        """Some FSEvents batches emit two displacement notifications."""
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
        # Two displacements in separate batches
        sm.process_events([ev('/tmp/b.txt', EventKind.RENAMED)])
        sm.emit()
        sm.process_events([ev('/tmp/b.txt', EventKind.RENAMED)])
        changes = sm.emit()
        # b.txt should NOT appear as a delete
        dels = [c for c in changes if c['op'] == 'delete']
        assert not any(d['path'] == '/tmp/b.txt' for d in dels)

    def test_displacement_does_not_block_real_rename(self, sm):
        """After displacement is absorbed, a real rename still works."""
        sm.process_events(
            [
                ev('/tmp/a.txt', EventKind.CREATED),
                ev('/tmp/b.txt', EventKind.CREATED),
                ev('/tmp/c.txt', EventKind.CREATED),
            ],
        )
        sm.emit()

        # Rename A -> B (overwrite)
        sm.process_events(
            [
                ev('/tmp/a.txt', EventKind.RENAMED),
                ev('/tmp/b.txt', EventKind.RENAMED),
            ],
        )
        # Displacement for B — absorbed by displacement detection
        sm.process_events([ev('/tmp/b.txt', EventKind.RENAMED)])
        # Grace emit: displacement was absorbed, no pending rename
        sm.emit()

        # Now a real rename: C -> D
        sm.process_events(
            [
                ev('/tmp/c.txt', EventKind.RENAMED),
                ev('/tmp/d.txt', EventKind.RENAMED),
            ],
        )
        changes = sm.emit()
        updates = [c['path'] for c in changes if c['op'] == 'update']
        assert '/tmp/d.txt' in updates


class TestParentChild:
    def test_create_child_before_parent(self, sm):
        # Create child file when parent directory is not yet tracked
        sm.process_events([ev('/tmp/dir/file.txt', EventKind.CREATED)])
        assert '/tmp/dir/file.txt' in sm.files
        # Parent /tmp/dir not in files, but shouldn't crash
        assert '/tmp/dir' not in sm.files

        # Now create parent
        sm.process_events([ev('/tmp/dir', EventKind.CREATED, is_dir=True)])
        assert '/tmp/dir' in sm.files


# ---------------------------------------------------------------------------
# Remove and update interaction
# ---------------------------------------------------------------------------


class TestRemoveAndUpdate:
    def test_remove_clears_to_update(self, sm):
        sm.process_events([ev('/tmp/a.txt', EventKind.CREATED)])
        assert '/tmp/a.txt' in sm.to_update

        sm.process_events([ev('/tmp/a.txt', EventKind.REMOVED)])
        assert '/tmp/a.txt' not in sm.to_update
        assert '/tmp/a.txt' in sm.to_delete

    def test_very_deep_directory_removal(self, sm):
        # Create 10-level deep nested directories
        paths = []
        for depth in range(10):
            dir_path = '/tmp/' + '/'.join(f'd{i}' for i in range(depth + 1))
            sm.process_events([ev(dir_path, EventKind.CREATED, is_dir=True)])
            paths.append(dir_path)

        # Add a leaf file
        leaf = paths[-1] + '/leaf.txt'
        sm.process_events([ev(leaf, EventKind.CREATED)])
        paths.append(leaf)

        # Remove root directory
        sm.process_events([ev('/tmp/d0', EventKind.REMOVED, is_dir=True)])

        # All descendants should be removed
        for p in paths:
            assert p not in sm.files, f'{p} should have been removed'


# ---------------------------------------------------------------------------
# Stat path edge cases
# ---------------------------------------------------------------------------


class TestStatPath:
    def test_permission_denied_returns_none(self):
        def _fail(p):
            raise PermissionError('denied')

        sm = StateManager(stat_fn=_fail)
        sm.process_events([ev('/tmp/a.txt', EventKind.CREATED)])
        changes = sm.emit()
        # Stat failure skips the event
        assert len(changes) == 0


# ---------------------------------------------------------------------------
# Emit edge cases
# ---------------------------------------------------------------------------


class TestEmitEdgeCases:
    def test_mixed_updates_and_deletes(self, sm):
        sm.process_events(
            [
                ev('/tmp/a.txt', EventKind.CREATED),
                ev('/tmp/b.txt', EventKind.CREATED),
            ],
        )
        # Emit both
        changes1 = sm.emit()
        assert len(changes1) == 2

        # Modify A, delete B
        sm.process_events(
            [
                ev('/tmp/a.txt', EventKind.MODIFIED),
                ev('/tmp/b.txt', EventKind.REMOVED),
            ],
        )
        changes2 = sm.emit()
        ups = [c for c in changes2 if c['op'] == 'update']
        dels = [c for c in changes2 if c['op'] == 'delete']
        assert any(u['path'] == '/tmp/a.txt' for u in ups)
        assert any(d['path'] == '/tmp/b.txt' for d in dels)

    def test_delete_never_emitted_skipped(self, sm):
        # Create and remove without emit in between
        sm.process_events([ev('/tmp/x.txt', EventKind.CREATED)])
        sm.process_events([ev('/tmp/x.txt', EventKind.REMOVED)])
        changes = sm.emit()
        # Should have no delete (was never emitted)
        assert not any(c['op'] == 'delete' for c in changes)

    def test_pending_rename_flushed_after_grace_period(self, sm):
        sm.process_events([ev('/tmp/a.txt', EventKind.CREATED)])
        sm.emit()  # mark as emitted

        # Send a single RENAMED event (no pair)
        sm.process_events([ev('/tmp/a.txt', EventKind.RENAMED)])

        # First emit: pending rename gets a grace cycle (age 0→1)
        changes1 = sm.emit()
        assert not any(c['op'] == 'delete' for c in changes1)

        # Second emit: grace expired (age 1→2), flushed as removal
        changes2 = sm.emit()
        dels = [c for c in changes2 if c['op'] == 'delete']
        assert any(d['path'] == '/tmp/a.txt' for d in dels)


# ---------------------------------------------------------------------------
# Re-creation after deletion
# ---------------------------------------------------------------------------


class TestRecreationAfterDeletion:
    def test_remove_then_create_same_path_emitted(self, sm):
        """REMOVED then CREATED for previously emitted path — should emit
        update (file exists), NOT delete.
        """
        sm.process_events([ev('/tmp/f.txt', EventKind.CREATED)])
        sm.emit()  # mark as emitted

        sm.process_events(
            [
                ev('/tmp/f.txt', EventKind.REMOVED),
                ev('/tmp/f.txt', EventKind.CREATED),
            ],
        )

        assert '/tmp/f.txt' in sm.files
        assert '/tmp/f.txt' in sm.to_update

        changes = sm.emit()
        update_paths = {c['path'] for c in changes if c['op'] == 'update'}
        delete_paths = {c['path'] for c in changes if c['op'] == 'delete'}

        assert '/tmp/f.txt' in update_paths, (
            f'Expected update for re-created file, got: {changes}'
        )
        assert '/tmp/f.txt' not in delete_paths, (
            f'Should NOT delete file that still exists, got: {changes}'
        )

    def test_remove_create_never_emitted(self, sm):
        """REMOVED then CREATED for never-emitted path — update only."""
        sm.process_events([ev('/tmp/f.txt', EventKind.CREATED)])

        sm.process_events(
            [
                ev('/tmp/f.txt', EventKind.REMOVED),
                ev('/tmp/f.txt', EventKind.CREATED),
            ],
        )

        changes = sm.emit()
        assert len(changes) == 1, f'Expected 1 change, got {changes}'
        assert changes[0]['op'] == 'update'
        assert changes[0]['path'] == '/tmp/f.txt'

    def test_remove_create_dir_same_path(self, sm):
        """REMOVED then CREATED for a directory at same path."""
        sm.process_events([ev('/tmp/dir', EventKind.CREATED, is_dir=True)])
        sm.emit()

        sm.process_events(
            [
                ev('/tmp/dir', EventKind.REMOVED, is_dir=True),
                ev('/tmp/dir', EventKind.CREATED, is_dir=True),
            ],
        )

        assert '/tmp/dir' in sm.files
        changes = sm.emit()
        update_paths = {c['path'] for c in changes if c['op'] == 'update'}
        delete_paths = {c['path'] for c in changes if c['op'] == 'delete'}

        assert '/tmp/dir' in update_paths
        assert '/tmp/dir' not in delete_paths


# ---------------------------------------------------------------------------
# Cross-batch rename interactions
# ---------------------------------------------------------------------------


class TestCrossBatchRename:
    def test_rename_split_no_emit_pairs_correctly(self, sm):
        """Rename pair split across process_events calls WITHOUT emit
        between them — pending rename persists, pair completes.
        """
        sm.process_events([ev('/tmp/a.txt', EventKind.CREATED)])
        sm.emit()

        sm.process_events([ev('/tmp/a.txt', EventKind.RENAMED)])
        sm.process_events([ev('/tmp/b.txt', EventKind.RENAMED)])

        assert '/tmp/a.txt' not in sm.files
        assert '/tmp/b.txt' in sm.files

        changes = sm.emit()
        ops = {(c['op'], c['path']) for c in changes}
        assert ('delete', '/tmp/a.txt') in ops
        assert ('update', '/tmp/b.txt') in ops

    def test_rename_split_with_emit_pairs_correctly(self, sm):
        """Rename pair split with emit() between — pending rename survives
        one grace cycle, then pairs with the second RENAMED event.
        """
        sm.process_events([ev('/tmp/a.txt', EventKind.CREATED)])
        sm.emit()

        sm.process_events([ev('/tmp/a.txt', EventKind.RENAMED)])
        changes1 = sm.emit()  # grace cycle: pending rename survives

        # Pending rename NOT flushed yet (grace period)
        assert not any(
            c['op'] == 'delete' and c['path'] == '/tmp/a.txt' for c in changes1
        )

        # Second event arrives in next batch — pairs correctly
        sm.process_events([ev('/tmp/b.txt', EventKind.RENAMED)])
        changes2 = sm.emit()

        # Rename completed: a.txt → b.txt
        assert '/tmp/b.txt' in sm.files
        updates = [c['path'] for c in changes2 if c['op'] == 'update']
        assert '/tmp/b.txt' in updates


# ---------------------------------------------------------------------------
# Unpaired rename edge cases
# ---------------------------------------------------------------------------


class TestUnpairedRenameEdgeCases:
    def test_three_consecutive_renames(self, sm):
        """3 RENAMED: first two pair, third orphaned — flushed after grace."""
        sm.process_events(
            [ev('/a', EventKind.CREATED), ev('/c', EventKind.CREATED)],
        )
        sm.emit()

        sm.process_events(
            [
                ev('/a', EventKind.RENAMED),
                ev('/b', EventKind.RENAMED),
                ev('/c', EventKind.RENAMED),
            ],
        )

        assert '/b' in sm.files
        assert sm._rename.pending is not None

        # First emit: /c gets grace cycle, /a deleted (paired rename)
        changes1 = sm.emit()
        delete_paths1 = {c['path'] for c in changes1 if c['op'] == 'delete'}
        assert '/a' in delete_paths1
        assert '/c' not in delete_paths1  # grace period

        # Second emit: grace expired, /c flushed as removal
        changes2 = sm.emit()
        delete_paths2 = {c['path'] for c in changes2 if c['op'] == 'delete'}
        assert '/c' in delete_paths2

    def test_renamed_followed_by_created(self, sm):
        """RENAMED then CREATED — pending rename flushed as removal."""
        sm.process_events([ev('/old.txt', EventKind.CREATED)])
        sm.emit()

        sm.process_events(
            [
                ev('/old.txt', EventKind.RENAMED),
                ev('/new.txt', EventKind.CREATED),
            ],
        )

        assert '/old.txt' not in sm.files
        assert '/new.txt' in sm.files

        changes = sm.emit()
        assert any(
            c['op'] == 'delete' and c['path'] == '/old.txt' for c in changes
        )
        assert any(
            c['op'] == 'update' and c['path'] == '/new.txt' for c in changes
        )

    def test_four_renames_two_pairs(self, sm):
        """4 RENAMED → 2 pairs: A→B and C→D."""
        sm.process_events(
            [ev('/a', EventKind.CREATED), ev('/c', EventKind.CREATED)],
        )

        sm.process_events(
            [
                ev('/a', EventKind.RENAMED),
                ev('/b', EventKind.RENAMED),
                ev('/c', EventKind.RENAMED),
                ev('/d', EventKind.RENAMED),
            ],
        )

        assert '/b' in sm.files
        assert '/d' in sm.files
        assert '/a' not in sm.files
        assert '/c' not in sm.files


# ---------------------------------------------------------------------------
# Nested rename + modify child
# ---------------------------------------------------------------------------


class TestNestedRenameModifyChild:
    def test_rename_dir_then_modify_child_at_new_path(self, sm):
        """Rename /a → /b, then MODIFIED /b/child.txt — state should track it."""
        sm.process_events(
            [
                ev('/a', EventKind.CREATED, is_dir=True),
                ev('/a/child.txt', EventKind.CREATED),
            ],
        )
        sm.emit()
        sm.to_update.clear()

        sm.process_events(
            [
                ev('/a', EventKind.RENAMED, is_dir=True),
                ev('/b', EventKind.RENAMED, is_dir=True),
            ],
        )
        sm.process_events([ev('/b/child.txt', EventKind.MODIFIED)])

        assert '/b/child.txt' in sm.to_update
        assert sm.files['/b/child.txt']['parent'] == '/b'

        changes = sm.emit()
        update_paths = {c['path'] for c in changes if c['op'] == 'update'}
        delete_paths = {c['path'] for c in changes if c['op'] == 'delete'}

        assert '/b/child.txt' in update_paths
        assert '/b' in update_paths
        assert '/a' in delete_paths
        assert '/a/child.txt' in delete_paths

    def test_rename_dir_modify_deeply_nested_child(self, sm):
        """Rename /a → /b, then MODIFIED /b/sub/deep/file.txt — verify
        parent chain is correct at all levels.
        """
        sm.process_events(
            [
                ev('/a', EventKind.CREATED, is_dir=True),
                ev('/a/sub', EventKind.CREATED, is_dir=True),
                ev('/a/sub/deep', EventKind.CREATED, is_dir=True),
                ev('/a/sub/deep/file.txt', EventKind.CREATED),
            ],
        )
        sm.emit()
        sm.to_update.clear()

        sm.process_events(
            [
                ev('/a', EventKind.RENAMED, is_dir=True),
                ev('/b', EventKind.RENAMED, is_dir=True),
            ],
        )

        assert sm.files['/b/sub/deep/file.txt']['parent'] == '/b/sub/deep'
        assert sm.files['/b/sub/deep']['parent'] == '/b/sub'
        assert sm.files['/b/sub']['parent'] == '/b'

        sm.process_events([ev('/b/sub/deep/file.txt', EventKind.MODIFIED)])

        changes = sm.emit()
        delete_paths = {c['path'] for c in changes if c['op'] == 'delete'}
        update_paths = {c['path'] for c in changes if c['op'] == 'update'}

        assert {
            '/a',
            '/a/sub',
            '/a/sub/deep',
            '/a/sub/deep/file.txt',
        } <= delete_paths
        assert {
            '/b',
            '/b/sub',
            '/b/sub/deep',
            '/b/sub/deep/file.txt',
        } <= update_paths


# ---------------------------------------------------------------------------
# Deep rename emit tracking
# ---------------------------------------------------------------------------


class TestDeepRenameEmitTracking:
    def test_3_level_rename_exact_delete_update_sets(self, sm):
        """Rename 3-level dir after emit — verify exact delete/update sets."""
        sm.process_events(
            [
                ev('/root', EventKind.CREATED, is_dir=True),
                ev('/root/a', EventKind.CREATED, is_dir=True),
                ev('/root/a/f1.txt', EventKind.CREATED),
                ev('/root/a/sub', EventKind.CREATED, is_dir=True),
                ev('/root/a/sub/f2.txt', EventKind.CREATED),
                ev('/root/a/sub/deep', EventKind.CREATED, is_dir=True),
                ev('/root/a/sub/deep/f3.txt', EventKind.CREATED),
            ],
        )
        changes1 = sm.emit()
        assert len(changes1) == 7

        sm.process_events(
            [
                ev('/root/a', EventKind.RENAMED, is_dir=True),
                ev('/root/z', EventKind.RENAMED, is_dir=True),
            ],
        )

        changes2 = sm.emit()
        delete_paths = {c['path'] for c in changes2 if c['op'] == 'delete'}
        update_paths = {c['path'] for c in changes2 if c['op'] == 'update'}

        expected_deletes = {
            '/root/a',
            '/root/a/f1.txt',
            '/root/a/sub',
            '/root/a/sub/f2.txt',
            '/root/a/sub/deep',
            '/root/a/sub/deep/f3.txt',
        }
        expected_updates = {
            '/root',
            '/root/z',
            '/root/z/f1.txt',
            '/root/z/sub',
            '/root/z/sub/f2.txt',
            '/root/z/sub/deep',
            '/root/z/sub/deep/f3.txt',
        }

        assert delete_paths == expected_deletes, (
            f'Deletes: expected {expected_deletes}, got {delete_paths}'
        )
        assert update_paths == expected_updates, (
            f'Updates: expected {expected_updates}, got {update_paths}'
        )

    def test_emitted_set_correct_after_rename(self, sm):
        """After rename + emit, emitted set has new paths only."""
        sm.process_events(
            [
                ev('/a', EventKind.CREATED, is_dir=True),
                ev('/a/f.txt', EventKind.CREATED),
            ],
        )
        sm.emit()

        sm.process_events(
            [
                ev('/a', EventKind.RENAMED, is_dir=True),
                ev('/b', EventKind.RENAMED, is_dir=True),
            ],
        )
        sm.emit()

        assert '/b' in sm.emitted
        assert '/b/f.txt' in sm.emitted
        assert '/a' not in sm.emitted
        assert '/a/f.txt' not in sm.emitted


# ---------------------------------------------------------------------------
# Rename collision: dir with children
# ---------------------------------------------------------------------------


class TestRenameCollisionDirWithChildren:
    def test_rename_dir_over_existing_dir_with_children(self, sm):
        """Rename dir A → dir B where B has children — B's tree removed."""
        sm.process_events(
            [
                ev('/B', EventKind.CREATED, is_dir=True),
                ev('/B/old1.txt', EventKind.CREATED),
                ev('/B/old2.txt', EventKind.CREATED),
                ev('/B/sub', EventKind.CREATED, is_dir=True),
                ev('/B/sub/old3.txt', EventKind.CREATED),
            ],
        )
        sm.process_events(
            [
                ev('/A', EventKind.CREATED, is_dir=True),
                ev('/A/new1.txt', EventKind.CREATED),
                ev('/A/new2.txt', EventKind.CREATED),
            ],
        )
        sm.emit()

        sm.process_events(
            [
                ev('/A', EventKind.RENAMED, is_dir=True),
                ev('/B', EventKind.RENAMED, is_dir=True),
            ],
        )

        # B's old children gone
        assert '/B/old1.txt' not in sm.files
        assert '/B/old2.txt' not in sm.files
        assert '/B/sub' not in sm.files
        assert '/B/sub/old3.txt' not in sm.files

        # A's children now under B
        assert '/B/new1.txt' in sm.files
        assert '/B/new2.txt' in sm.files
        assert '/A' not in sm.files

        changes = sm.emit()
        delete_paths = {c['path'] for c in changes if c['op'] == 'delete'}
        update_paths = {c['path'] for c in changes if c['op'] == 'update'}

        assert '/A' in delete_paths
        assert '/B/old1.txt' in delete_paths
        assert '/B/old2.txt' in delete_paths
        assert '/B/sub' in delete_paths
        assert '/B/sub/old3.txt' in delete_paths
        assert '/B' in update_paths
        assert '/B/new1.txt' in update_paths
        assert '/B/new2.txt' in update_paths

    def test_rename_file_over_dir_with_children(self, sm):
        """Rename file A → path B where B is dir with children."""
        sm.process_events(
            [
                ev('/B', EventKind.CREATED, is_dir=True),
                ev('/B/child.txt', EventKind.CREATED),
            ],
        )
        sm.process_events([ev('/A.txt', EventKind.CREATED)])
        sm.emit()

        sm.process_events(
            [
                ev('/A.txt', EventKind.RENAMED),
                ev('/B', EventKind.RENAMED),
            ],
        )

        assert '/B' in sm.files
        assert sm.files['/B']['is_dir'] is False
        assert '/B/child.txt' not in sm.files

        changes = sm.emit()
        delete_paths = {c['path'] for c in changes if c['op'] == 'delete'}
        update_paths = {c['path'] for c in changes if c['op'] == 'update'}

        assert '/A.txt' in delete_paths
        assert '/B/child.txt' in delete_paths
        assert '/B' in update_paths


# ---------------------------------------------------------------------------
# Circular / swap renames
# ---------------------------------------------------------------------------


class TestCircularRename:
    def test_pairwise_circular_overwrites(self, sm):
        """Pairwise A→B, B→C, C→A: each overwrites target, only 1 survives."""
        sm.process_events(
            [
                ev('/A', EventKind.CREATED),
                ev('/B', EventKind.CREATED),
                ev('/C', EventKind.CREATED),
            ],
        )
        sm.emit()

        sm.process_events(
            [ev('/A', EventKind.RENAMED), ev('/B', EventKind.RENAMED)],
        )
        sm.process_events(
            [ev('/B', EventKind.RENAMED), ev('/C', EventKind.RENAMED)],
        )
        sm.process_events(
            [ev('/C', EventKind.RENAMED), ev('/A', EventKind.RENAMED)],
        )

        assert len(sm.files) == 1
        assert '/A' in sm.files

        changes = sm.emit()
        delete_paths = {c['path'] for c in changes if c['op'] == 'delete'}
        update_paths = {c['path'] for c in changes if c['op'] == 'update'}
        assert '/B' in delete_paths
        assert '/C' in delete_paths
        assert '/A' in update_paths

    def test_swap_via_temp(self, sm):
        """Proper swap via temp: A→tmp, B→A, tmp→B. Both survive."""
        sm.process_events(
            [ev('/A', EventKind.CREATED), ev('/B', EventKind.CREATED)],
        )
        sm.emit()

        sm.process_events(
            [ev('/A', EventKind.RENAMED), ev('/tmp', EventKind.RENAMED)],
        )
        sm.process_events(
            [ev('/B', EventKind.RENAMED), ev('/A', EventKind.RENAMED)],
        )
        sm.process_events(
            [ev('/tmp', EventKind.RENAMED), ev('/B', EventKind.RENAMED)],
        )

        assert len(sm.files) == 2
        assert '/A' in sm.files
        assert '/B' in sm.files

        changes = sm.emit()
        update_paths = {c['path'] for c in changes if c['op'] == 'update'}
        delete_paths = {c['path'] for c in changes if c['op'] == 'delete'}
        assert '/A' in update_paths
        assert '/B' in update_paths
        assert '/tmp' in delete_paths
        assert '/A' not in delete_paths
        assert '/B' not in delete_paths


# ---------------------------------------------------------------------------
# Stat failure race conditions
# ---------------------------------------------------------------------------


class TestStatFailureRace:
    def test_partial_stat_failure(self):
        """Some files stat-fail, others succeed — partial output."""

        def selective_stat(path):
            if path == '/gone.txt':
                raise OSError('No such file')
            return make_stat_result()

        sm = StateManager(stat_fn=selective_stat)
        sm.process_events(
            [
                ev('/exists.txt', EventKind.CREATED),
                ev('/gone.txt', EventKind.CREATED),
                ev('/also_exists.txt', EventKind.CREATED),
            ],
        )

        changes = sm.emit()

        update_paths = {c['path'] for c in changes if c['op'] == 'update'}
        assert '/exists.txt' in update_paths
        assert '/also_exists.txt' in update_paths
        assert '/gone.txt' not in update_paths
        assert '/gone.txt' in sm.files  # still tracked

    def test_all_stat_failures_empty_output(self):
        """All stats fail — emit produces nothing, files still tracked."""

        def _fail(p):
            raise OSError('gone')

        sm = StateManager(stat_fn=_fail)
        sm.process_events(
            [ev('/a.txt', EventKind.CREATED), ev('/b.txt', EventKind.CREATED)],
        )

        changes = sm.emit()

        assert len(changes) == 0
        assert '/a.txt' in sm.files
        assert '/b.txt' in sm.files

    def test_stat_failure_then_success_on_retry(self):
        """Stat fails first emit, succeeds on second after re-modification."""
        behavior = {'fail': True}

        def _stat(p):
            if behavior['fail']:
                raise OSError('busy')
            return make_stat_result()

        sm = StateManager(stat_fn=_stat)
        sm.process_events([ev('/flaky.txt', EventKind.CREATED)])

        assert len(sm.emit()) == 0

        behavior['fail'] = False
        sm.process_events([ev('/flaky.txt', EventKind.MODIFIED)])

        changes = sm.emit()
        assert len(changes) == 1
        assert changes[0]['op'] == 'update'

    def test_stat_failure_not_added_to_emitted(self):
        """Stat failure means path NOT in emitted set."""

        def _fail(p):
            raise OSError('gone')

        sm = StateManager(stat_fn=_fail)
        sm.process_events([ev('/a.txt', EventKind.CREATED)])

        sm.emit()

        assert '/a.txt' not in sm.emitted

    def test_delete_after_stat_failure_no_delete_event(self):
        """REMOVED after stat failure — no delete event (never emitted)."""
        behavior = {'fail': True}

        def _stat(p):
            if behavior['fail']:
                raise OSError('gone')
            return make_stat_result()

        sm = StateManager(stat_fn=_stat)
        sm.process_events([ev('/a.txt', EventKind.CREATED)])

        sm.emit()

        sm.process_events([ev('/a.txt', EventKind.REMOVED)])

        behavior['fail'] = False
        changes = sm.emit()

        assert not any(c['op'] == 'delete' for c in changes)


# ---------------------------------------------------------------------------
# Large fan-out rename
# ---------------------------------------------------------------------------


class TestLargeFanOutRename:
    def test_1000_files_rename_exact_counts(self, sm):
        """1000 files under /a/, emit, rename /a/ → /b/. Exact counts."""
        sm.process_events([ev('/a', EventKind.CREATED, is_dir=True)])
        sm.process_events(
            [ev(f'/a/file_{i}.txt', EventKind.CREATED) for i in range(1000)],
        )
        changes1 = sm.emit()
        assert len(changes1) == 1001

        sm.process_events(
            [
                ev('/a', EventKind.RENAMED, is_dir=True),
                ev('/b', EventKind.RENAMED, is_dir=True),
            ],
        )
        changes2 = sm.emit()
        deletes = [c for c in changes2 if c['op'] == 'delete']
        updates = [c for c in changes2 if c['op'] == 'update']
        assert len(deletes) == 1001
        assert len(updates) == 1001

    def test_1000_files_children_structure(self, sm):
        """After renaming dir with 1000 files, children dict correct."""
        sm.process_events([ev('/a', EventKind.CREATED, is_dir=True)])
        sm.process_events(
            [ev(f'/a/f_{i}', EventKind.CREATED) for i in range(1000)],
        )

        sm.process_events(
            [
                ev('/a', EventKind.RENAMED, is_dir=True),
                ev('/b', EventKind.RENAMED, is_dir=True),
            ],
        )

        assert '/a' not in sm.children
        assert len(sm.children['/b']) == 1000


# ---------------------------------------------------------------------------
# Multi-emit cycles with renames
# ---------------------------------------------------------------------------


class TestMultiEmitCycles:
    def test_create_rename_delete_three_emits(self, sm):
        """Create → emit → rename → emit → delete → emit."""
        sm.process_events([ev('/f.txt', EventKind.CREATED)])
        changes1 = sm.emit()
        assert len(changes1) == 1
        assert changes1[0]['op'] == 'update'

        sm.process_events(
            [
                ev('/f.txt', EventKind.RENAMED),
                ev('/g.txt', EventKind.RENAMED),
            ],
        )
        changes2 = sm.emit()
        assert len(changes2) == 2
        assert {(c['op'], c['path']) for c in changes2} == {
            ('delete', '/f.txt'),
            ('update', '/g.txt'),
        }

        sm.process_events([ev('/g.txt', EventKind.REMOVED)])
        changes3 = sm.emit()
        assert len(changes3) == 1
        assert changes3[0] == {
            'op': 'delete',
            'fid': '/g.txt',
            'path': '/g.txt',
        }

    def test_rename_then_create_old_path(self, sm):
        """Rename away then re-create at old path — old path gets update, not delete."""
        sm.process_events([ev('/a.txt', EventKind.CREATED)])
        sm.emit()

        sm.process_events(
            [
                ev('/a.txt', EventKind.RENAMED),
                ev('/b.txt', EventKind.RENAMED),
            ],
        )
        sm.process_events([ev('/a.txt', EventKind.CREATED)])

        changes = sm.emit()
        update_paths = {c['path'] for c in changes if c['op'] == 'update'}
        delete_paths = {c['path'] for c in changes if c['op'] == 'delete'}

        assert '/b.txt' in update_paths
        assert '/a.txt' in update_paths
        assert '/a.txt' not in delete_paths


# ---------------------------------------------------------------------------
# Concurrent dir removal while children created
# ---------------------------------------------------------------------------


class TestConcurrentDirRemoval:
    def test_create_child_then_remove_dir(self, sm):
        """CREATED child + REMOVED dir in same batch — child cascade-removed."""
        sm.process_events([ev('/dir', EventKind.CREATED, is_dir=True)])
        sm.emit()

        sm.process_events(
            [
                ev('/dir/new_child.txt', EventKind.CREATED),
                ev('/dir', EventKind.REMOVED, is_dir=True),
            ],
        )

        assert '/dir' not in sm.files
        assert '/dir/new_child.txt' not in sm.files

        changes = sm.emit()
        delete_paths = {c['path'] for c in changes if c['op'] == 'delete'}
        assert '/dir' in delete_paths
        assert '/dir/new_child.txt' not in delete_paths  # never emitted

    def test_remove_dir_existing_and_new_children(self, sm):
        """Dir with emitted children + new child + remove.
        Emitted children get deletes, new child doesn't.
        """
        sm.process_events(
            [
                ev('/dir', EventKind.CREATED, is_dir=True),
                ev('/dir/existing.txt', EventKind.CREATED),
            ],
        )
        sm.emit()

        sm.process_events(
            [
                ev('/dir/new.txt', EventKind.CREATED),
                ev('/dir', EventKind.REMOVED, is_dir=True),
            ],
        )

        changes = sm.emit()
        delete_paths = {c['path'] for c in changes if c['op'] == 'delete'}
        assert '/dir' in delete_paths
        assert '/dir/existing.txt' in delete_paths
        assert '/dir/new.txt' not in delete_paths


# ---------------------------------------------------------------------------
# Rename chain with intermediate collisions
# ---------------------------------------------------------------------------


class TestRenameChainCollision:
    def test_rename_to_existing_then_rename_again(self, sm):
        """A→B (overwrites B), then B→C. Only C survives."""
        sm.process_events(
            [ev('/A', EventKind.CREATED), ev('/B', EventKind.CREATED)],
        )
        sm.emit()

        sm.process_events(
            [ev('/A', EventKind.RENAMED), ev('/B', EventKind.RENAMED)],
        )
        sm.process_events(
            [ev('/B', EventKind.RENAMED), ev('/C', EventKind.RENAMED)],
        )

        assert '/C' in sm.files
        assert '/A' not in sm.files
        assert '/B' not in sm.files

        changes = sm.emit()
        delete_paths = {c['path'] for c in changes if c['op'] == 'delete'}
        update_paths = {c['path'] for c in changes if c['op'] == 'update'}
        assert '/A' in delete_paths
        assert '/B' in delete_paths
        assert '/C' in update_paths

    def test_three_way_collision_chain(self, sm):
        """A→B→C→D with collisions at each step. Only D survives."""
        sm.process_events(
            [
                ev('/A', EventKind.CREATED),
                ev('/B', EventKind.CREATED),
                ev('/C', EventKind.CREATED),
            ],
        )
        sm.emit()

        sm.process_events(
            [ev('/A', EventKind.RENAMED), ev('/B', EventKind.RENAMED)],
        )
        sm.process_events(
            [ev('/B', EventKind.RENAMED), ev('/C', EventKind.RENAMED)],
        )
        sm.process_events(
            [ev('/C', EventKind.RENAMED), ev('/D', EventKind.RENAMED)],
        )

        assert len(sm.files) == 1
        assert '/D' in sm.files

        changes = sm.emit()
        delete_paths = {c['path'] for c in changes if c['op'] == 'delete'}
        assert {'/A', '/B', '/C'} <= delete_paths

    def test_log_rotation_rename_and_recreate(self, sm):
        """Log rotation: rename log→log.1, re-create log."""
        sm.process_events([ev('/app.log', EventKind.CREATED)])
        sm.emit()

        sm.process_events(
            [
                ev('/app.log', EventKind.RENAMED),
                ev('/app.log.1', EventKind.RENAMED),
            ],
        )
        sm.process_events([ev('/app.log', EventKind.CREATED)])

        assert '/app.log' in sm.files
        assert '/app.log.1' in sm.files

        changes = sm.emit()
        update_paths = {c['path'] for c in changes if c['op'] == 'update'}
        delete_paths = {c['path'] for c in changes if c['op'] == 'delete'}
        assert '/app.log' in update_paths
        assert '/app.log.1' in update_paths
        assert '/app.log' not in delete_paths  # cleared by re-creation

    def test_double_rotation(self, sm):
        """log.1→log.2, log→log.1, create new log. All 3 survive."""
        sm.process_events(
            [
                ev('/app.log', EventKind.CREATED),
                ev('/app.log.1', EventKind.CREATED),
            ],
        )
        sm.emit()

        sm.process_events(
            [
                ev('/app.log.1', EventKind.RENAMED),
                ev('/app.log.2', EventKind.RENAMED),
            ],
        )
        sm.process_events(
            [
                ev('/app.log', EventKind.RENAMED),
                ev('/app.log.1', EventKind.RENAMED),
            ],
        )
        sm.process_events([ev('/app.log', EventKind.CREATED)])

        assert len(sm.files) == 3
        changes = sm.emit()
        update_paths = {c['path'] for c in changes if c['op'] == 'update'}
        assert '/app.log' in update_paths
        assert '/app.log.1' in update_paths
        assert '/app.log.2' in update_paths


# ---------------------------------------------------------------------------
# Rename with unknown paths
# ---------------------------------------------------------------------------


class TestRenameUnknownPaths:
    def test_rename_pair_both_unknown(self, sm):
        """Both old and new paths unknown — treated as creation at new."""
        sm.process_events(
            [
                ev('/unknown_old', EventKind.RENAMED),
                ev('/unknown_new', EventKind.RENAMED),
            ],
        )
        assert '/unknown_new' in sm.files
        assert len(sm.files) == 1

    def test_rename_known_then_unknown_pair(self, sm):
        """Valid pair + unknown pair — both produce entries."""
        sm.process_events([ev('/a', EventKind.CREATED)])

        sm.process_events(
            [
                ev('/a', EventKind.RENAMED),
                ev('/b', EventKind.RENAMED),
                ev('/x', EventKind.RENAMED),
                ev('/y', EventKind.RENAMED),
            ],
        )

        assert '/b' in sm.files
        assert '/y' in sm.files
        assert len(sm.files) == 2

    def test_unpaired_rename_unknown(self, sm):
        """Single RENAMED for unknown path → no-op removal."""
        sm.process_events([ev('/unknown', EventKind.RENAMED)])
        assert len(sm.emit()) == 0


# ---------------------------------------------------------------------------
# Attribute change + rename in state
# ---------------------------------------------------------------------------


class TestAttrRenameState:
    def test_attr_change_then_rename(self, sm):
        """MODIFIED (chmod) then rename — both processed."""
        sm.process_events([ev('/file.txt', EventKind.CREATED)])
        sm.emit()

        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events(
            [
                ev('/file.txt', EventKind.MODIFIED),
                ev('/file.txt', EventKind.RENAMED),
                ev('/renamed.txt', EventKind.RENAMED),
            ],
        )
        sm.process_events(bp.flush())

        assert '/renamed.txt' in sm.files
        changes = sm.emit()
        delete_paths = {c['path'] for c in changes if c['op'] == 'delete'}
        update_paths = {c['path'] for c in changes if c['op'] == 'update'}
        assert '/file.txt' in delete_paths
        assert '/renamed.txt' in update_paths


# ---------------------------------------------------------------------------
# Deep parent field verification
# ---------------------------------------------------------------------------


class TestDeepParentField:
    def test_5_level_parent_correct_after_rename(self, sm):
        """5-level deep tree rename — verify parent field at every level."""
        sm.process_events(
            [
                ev('/root', EventKind.CREATED, is_dir=True),
                ev('/root/L1', EventKind.CREATED, is_dir=True),
                ev('/root/L1/L2', EventKind.CREATED, is_dir=True),
                ev('/root/L1/L2/L3', EventKind.CREATED, is_dir=True),
                ev('/root/L1/L2/L3/L4', EventKind.CREATED, is_dir=True),
                ev('/root/L1/L2/L3/L4/leaf.txt', EventKind.CREATED),
            ],
        )
        sm.process_events(
            [
                ev('/root', EventKind.RENAMED, is_dir=True),
                ev('/newroot', EventKind.RENAMED, is_dir=True),
            ],
        )

        assert sm.files['/newroot']['parent'] == ''
        assert sm.files['/newroot/L1']['parent'] == '/newroot'
        assert sm.files['/newroot/L1/L2']['parent'] == '/newroot/L1'
        assert sm.files['/newroot/L1/L2/L3']['parent'] == '/newroot/L1/L2'
        assert (
            sm.files['/newroot/L1/L2/L3/L4']['parent'] == '/newroot/L1/L2/L3'
        )
        assert (
            sm.files['/newroot/L1/L2/L3/L4/leaf.txt']['parent']
            == '/newroot/L1/L2/L3/L4'
        )

    def test_nested_rename_then_rename_child_dir(self, sm):
        """Rename top dir, then rename child dir within new tree."""
        sm.process_events(
            [
                ev('/a', EventKind.CREATED, is_dir=True),
                ev('/a/sub', EventKind.CREATED, is_dir=True),
                ev('/a/sub/file.txt', EventKind.CREATED),
            ],
        )
        sm.process_events(
            [
                ev('/a', EventKind.RENAMED, is_dir=True),
                ev('/b', EventKind.RENAMED, is_dir=True),
            ],
        )
        sm.process_events(
            [
                ev('/b/sub', EventKind.RENAMED, is_dir=True),
                ev('/b/newsub', EventKind.RENAMED, is_dir=True),
            ],
        )

        assert '/b/newsub/file.txt' in sm.files
        assert sm.files['/b/newsub/file.txt']['parent'] == '/b/newsub'

    def test_children_dict_at_all_levels(self, sm):
        """After rename, children dict entries correct at every level."""
        sm.process_events(
            [
                ev('/a', EventKind.CREATED, is_dir=True),
                ev('/a/b', EventKind.CREATED, is_dir=True),
                ev('/a/b/c', EventKind.CREATED, is_dir=True),
                ev('/a/b/c/f.txt', EventKind.CREATED),
                ev('/a/b/f2.txt', EventKind.CREATED),
                ev('/a/f3.txt', EventKind.CREATED),
            ],
        )
        sm.process_events(
            [
                ev('/a', EventKind.RENAMED, is_dir=True),
                ev('/z', EventKind.RENAMED, is_dir=True),
            ],
        )

        assert '/z/b' in sm.children['/z']
        assert '/z/f3.txt' in sm.children['/z']
        assert '/z/b/c' in sm.children['/z/b']
        assert '/z/b/f2.txt' in sm.children['/z/b']
        assert '/z/b/c/f.txt' in sm.children['/z/b/c']


# ---------------------------------------------------------------------------
# Rename to self
# ---------------------------------------------------------------------------


class TestRenameToSelf:
    def test_rename_self_emitted_gets_update(self, sm):
        """Rename A→A after emit — should get update, not delete."""
        sm.process_events([ev('/a.txt', EventKind.CREATED)])
        sm.emit()

        sm.process_events(
            [ev('/a.txt', EventKind.RENAMED), ev('/a.txt', EventKind.RENAMED)],
        )
        changes = sm.emit()
        update_paths = {c['path'] for c in changes if c['op'] == 'update'}
        assert '/a.txt' in update_paths

    def test_rename_self_preserves_entry(self, sm):
        """Rename A→A preserves all entry fields, updates ts."""
        sm.process_events(
            [ev('/a.txt', EventKind.CREATED, is_symlink=True, ts=42.0)],
        )
        sm.process_events(
            [
                ev('/a.txt', EventKind.RENAMED),
                ev('/a.txt', EventKind.RENAMED, ts=99.0),
            ],
        )
        assert sm.files['/a.txt']['is_symlink'] is True
        assert sm.files['/a.txt']['ts'] == 99.0

    def test_rename_dir_self_children_survive(self, sm):
        """Rename dir A→A — children survive."""
        sm.process_events(
            [
                ev('/dir', EventKind.CREATED, is_dir=True),
                ev('/dir/f.txt', EventKind.CREATED),
            ],
        )
        sm.process_events(
            [
                ev('/dir', EventKind.RENAMED, is_dir=True),
                ev('/dir', EventKind.RENAMED, is_dir=True),
            ],
        )
        assert '/dir' in sm.files
        assert '/dir/f.txt' in sm.files


# ---------------------------------------------------------------------------
# Untracked path interactions
# ---------------------------------------------------------------------------


class TestUntrackedPathInteractions:
    def test_remove_untracked_dir_then_create_children(self, sm):
        """REMOVED untracked dir + CREATED children — children tracked."""
        sm.process_events(
            [
                ev('/untracked', EventKind.REMOVED, is_dir=True),
                ev('/new', EventKind.CREATED, is_dir=True),
                ev('/new/file.txt', EventKind.CREATED),
            ],
        )
        assert '/new/file.txt' in sm.files
        assert len(sm.emit()) == 2

    def test_modify_untracked_ignored(self, sm):
        """MODIFIED for untracked file — ignored."""
        sm.process_events([ev('/x', EventKind.MODIFIED)])
        assert len(sm.emit()) == 0

    def test_remove_untracked_then_create_same_path(self, sm):
        """REMOVED untracked + CREATED same path."""
        sm.process_events(
            [ev('/p', EventKind.REMOVED), ev('/p', EventKind.CREATED)],
        )
        changes = sm.emit()
        assert len(changes) == 1
        assert changes[0]['op'] == 'update'

    def test_accessed_untracked_ignored(self, sm):
        """ACCESSED untracked — ignored."""
        sm.process_events([ev('/x', EventKind.MODIFIED)])
        assert len(sm.emit()) == 0


# ---------------------------------------------------------------------------
# CREATED overwrite (same path, different type)
# ---------------------------------------------------------------------------


class TestCreatedOverwrite:
    def test_created_dir_then_file_same_path(self, sm):
        """CREATED as dir then CREATED as file — overwrites to file."""
        sm.process_events(
            [
                ev('/path', EventKind.CREATED, is_dir=True),
                ev('/path', EventKind.CREATED, is_dir=False),
            ],
        )
        assert sm.files['/path']['is_dir'] is False

    def test_created_file_then_dir_same_path(self, sm):
        """CREATED as file then CREATED as dir — overwrites to dir."""
        sm.process_events(
            [
                ev('/path', EventKind.CREATED, is_dir=False),
                ev('/path', EventKind.CREATED, is_dir=True),
            ],
        )
        assert sm.files['/path']['is_dir'] is True

    def test_created_twice_same_type_updates_ts(self, sm):
        """CREATED twice as file — second overwrites, ts updated."""
        sm.process_events(
            [
                ev('/f.txt', EventKind.CREATED, ts=1.0),
                ev('/f.txt', EventKind.CREATED, ts=2.0),
            ],
        )
        assert sm.files['/f.txt']['ts'] == 2.0
        assert len(sm.emit()) == 1


# ---------------------------------------------------------------------------
# Multi-emit subtree removal
# ---------------------------------------------------------------------------


class TestMultiEmitSubtreeRemoval:
    def test_incremental_children_all_deleted(self, sm):
        """Create tree → emit → add children → emit → rmtree → emit.
        All emitted paths get deletes.
        """
        sm.process_events(
            [
                ev('/tree', EventKind.CREATED, is_dir=True),
                ev('/tree/a.txt', EventKind.CREATED),
            ],
        )
        sm.emit()

        sm.process_events(
            [
                ev('/tree/sub', EventKind.CREATED, is_dir=True),
                ev('/tree/sub/b.txt', EventKind.CREATED),
                ev('/tree/c.txt', EventKind.CREATED),
            ],
        )
        sm.emit()

        sm.process_events([ev('/tree', EventKind.REMOVED, is_dir=True)])
        changes = sm.emit()
        delete_paths = {c['path'] for c in changes if c['op'] == 'delete'}
        assert delete_paths == {
            '/tree',
            '/tree/a.txt',
            '/tree/sub',
            '/tree/sub/b.txt',
            '/tree/c.txt',
        }

    def test_partial_then_full_removal(self, sm):
        """Remove one child → emit → rmtree → emit. No double deletes."""
        sm.process_events(
            [
                ev('/dir', EventKind.CREATED, is_dir=True),
                ev('/dir/keep.txt', EventKind.CREATED),
                ev('/dir/rm.txt', EventKind.CREATED),
            ],
        )
        sm.emit()

        sm.process_events([ev('/dir/rm.txt', EventKind.REMOVED)])
        sm.emit()

        sm.process_events([ev('/dir', EventKind.REMOVED, is_dir=True)])
        changes = sm.emit()
        delete_paths = {c['path'] for c in changes if c['op'] == 'delete'}
        assert '/dir' in delete_paths
        assert '/dir/keep.txt' in delete_paths
        assert '/dir/rm.txt' not in delete_paths  # already deleted


# ---------------------------------------------------------------------------
# Rename then remove directory
# ---------------------------------------------------------------------------


class TestRenameThenRemoveDir:
    def test_rename_then_remove_all_cleaned(self, sm):
        """Rename dir A→B, remove B. All paths gone, all deletes emitted."""
        sm.process_events(
            [
                ev('/A', EventKind.CREATED, is_dir=True),
                ev('/A/f1.txt', EventKind.CREATED),
                ev('/A/f2.txt', EventKind.CREATED),
            ],
        )
        sm.emit()

        sm.process_events(
            [
                ev('/A', EventKind.RENAMED, is_dir=True),
                ev('/B', EventKind.RENAMED, is_dir=True),
            ],
        )
        sm.process_events([ev('/B', EventKind.REMOVED, is_dir=True)])

        assert len(sm.files) == 0
        changes = sm.emit()
        delete_paths = {c['path'] for c in changes if c['op'] == 'delete'}
        assert {
            '/A',
            '/A/f1.txt',
            '/A/f2.txt',
            '/B',
            '/B/f1.txt',
            '/B/f2.txt',
        } <= delete_paths

    def test_rename_modify_then_remove(self, sm):
        """Rename dir, modify child, remove dir."""
        sm.process_events(
            [
                ev('/src', EventKind.CREATED, is_dir=True),
                ev('/src/main.py', EventKind.CREATED),
            ],
        )
        sm.emit()

        sm.process_events(
            [
                ev('/src', EventKind.RENAMED, is_dir=True),
                ev('/dst', EventKind.RENAMED, is_dir=True),
            ],
        )
        sm.process_events([ev('/dst/main.py', EventKind.MODIFIED)])
        sm.process_events([ev('/dst', EventKind.REMOVED, is_dir=True)])

        assert len(sm.files) == 0
        changes = sm.emit()
        delete_paths = {c['path'] for c in changes if c['op'] == 'delete'}
        assert '/src' in delete_paths
        assert '/src/main.py' in delete_paths


# ---------------------------------------------------------------------------
# Self-nesting rename
# ---------------------------------------------------------------------------


class TestSelfNestingRename:
    def test_rename_dir_to_subpath_no_crash(self, sm):
        """Rename /a → /a/b — impossible FS op but should not crash."""
        sm.process_events(
            [
                ev('/a', EventKind.CREATED, is_dir=True),
                ev('/a/file.txt', EventKind.CREATED),
            ],
        )
        sm.process_events(
            [
                ev('/a', EventKind.RENAMED, is_dir=True),
                ev('/a/b', EventKind.RENAMED, is_dir=True),
            ],
        )
        assert '/a' not in sm.files or '/a/b' in sm.files

    def test_rename_file_to_longer_path(self, sm):
        """Rename /short → /much/longer/path."""
        sm.process_events([ev('/short', EventKind.CREATED)])
        sm.process_events(
            [
                ev('/short', EventKind.RENAMED),
                ev('/much/longer/path', EventKind.RENAMED),
            ],
        )
        assert '/much/longer/path' in sm.files
        assert sm.files['/much/longer/path']['parent'] == '/much/longer'


# ---------------------------------------------------------------------------
# Empty directory operations
# ---------------------------------------------------------------------------


class TestEmptyDirOps:
    def test_create_emit_remove_empty_dir(self, sm):
        """Create empty dir → emit → remove → emit."""
        sm.process_events([ev('/empty', EventKind.CREATED, is_dir=True)])
        assert len(sm.emit()) == 1
        sm.process_events([ev('/empty', EventKind.REMOVED, is_dir=True)])
        changes = sm.emit()
        assert len(changes) == 1
        assert changes[0] == {
            'op': 'delete',
            'fid': '/empty',
            'path': '/empty',
        }

    def test_dir_becomes_empty_then_removed(self, sm):
        """Create dir + file → emit → rm file → emit → rm dir → emit."""
        sm.process_events(
            [
                ev('/dir', EventKind.CREATED, is_dir=True),
                ev('/dir/f.txt', EventKind.CREATED),
            ],
        )
        sm.emit()
        sm.process_events([ev('/dir/f.txt', EventKind.REMOVED)])
        sm.emit()
        sm.process_events([ev('/dir', EventKind.REMOVED, is_dir=True)])
        changes = sm.emit()
        assert any(
            c['op'] == 'delete' and c['path'] == '/dir' for c in changes
        )

    def test_nested_empty_dirs_remove_leaf(self, sm):
        """Nested empty dirs → remove leaf → parent gets update."""
        sm.process_events(
            [
                ev('/a', EventKind.CREATED, is_dir=True),
                ev('/a/b', EventKind.CREATED, is_dir=True),
                ev('/a/b/c', EventKind.CREATED, is_dir=True),
            ],
        )
        sm.emit()
        sm.process_events([ev('/a/b/c', EventKind.REMOVED, is_dir=True)])
        changes = sm.emit()
        assert any(
            c['op'] == 'delete' and c['path'] == '/a/b/c' for c in changes
        )
        assert any(
            c['op'] == 'update' and c['path'] == '/a/b' for c in changes
        )

    def test_transient_dir_no_output(self, sm):
        """Create+remove dir without emit — no output."""
        sm.process_events(
            [
                ev('/t', EventKind.CREATED, is_dir=True),
                ev('/t', EventKind.REMOVED, is_dir=True),
            ],
        )
        assert len(sm.emit()) == 0


# ---------------------------------------------------------------------------
# Timestamp handling edge cases
# ---------------------------------------------------------------------------


class TestTimestampHandling:
    def test_none_timestamp(self, sm):
        """ts=None preserved."""
        sm.process_events([ev('/a', EventKind.CREATED, ts=None)])
        assert sm.files['/a']['ts'] is None

    def test_zero_timestamp(self, sm):
        """ts=0 is valid."""
        sm.process_events([ev('/a', EventKind.CREATED, ts=0.0)])
        assert sm.files['/a']['ts'] == 0.0

    def test_negative_timestamp(self, sm):
        """Negative ts — shouldn't crash."""
        sm.process_events([ev('/a', EventKind.CREATED, ts=-1.0)])
        assert sm.files['/a']['ts'] == -1.0

    def test_none_ts_modify_preserves_original(self, sm):
        """MODIFIED with ts=None doesn't overwrite existing ts."""
        sm.process_events([ev('/a', EventKind.CREATED, ts=100.0)])
        sm.process_events([ev('/a', EventKind.MODIFIED, ts=None)])
        assert sm.files['/a']['ts'] == 100.0

    def test_rename_updates_ts(self, sm):
        """Rename updates ts to second event's ts."""
        sm.process_events([ev('/a', EventKind.CREATED, ts=100.0)])
        sm.process_events(
            [
                ev('/a', EventKind.RENAMED, ts=200.0),
                ev('/b', EventKind.RENAMED, ts=300.0),
            ],
        )
        assert sm.files['/b']['ts'] == 300.0


# ---------------------------------------------------------------------------
# Parent-child batch interactions
# ---------------------------------------------------------------------------


class TestParentChildBatch:
    def test_create_dir_and_child_same_batch(self, sm):
        """CREATED dir + child same batch — both tracked, parent updated."""
        sm.process_events(
            [
                ev('/dir', EventKind.CREATED, is_dir=True),
                ev('/dir/file.txt', EventKind.CREATED),
            ],
        )
        assert '/dir/file.txt' in sm.children['/dir']
        changes = sm.emit()
        assert len(changes) == 2

    def test_create_child_updates_parent(self, sm):
        """Creating child after parent emitted — parent re-emitted."""
        sm.process_events([ev('/dir', EventKind.CREATED, is_dir=True)])
        sm.emit()
        sm.to_update.clear()

        sm.process_events([ev('/dir/new.txt', EventKind.CREATED)])
        assert '/dir' in sm.to_update

    def test_remove_child_updates_parent(self, sm):
        """Removing child — parent re-emitted."""
        sm.process_events(
            [
                ev('/dir', EventKind.CREATED, is_dir=True),
                ev('/dir/f.txt', EventKind.CREATED),
            ],
        )
        sm.emit()
        sm.to_update.clear()

        sm.process_events([ev('/dir/f.txt', EventKind.REMOVED)])
        assert '/dir' in sm.to_update


# ---------------------------------------------------------------------------
# Remove then rename INTO same path
# ---------------------------------------------------------------------------


class TestRemoveThenRenameInto:
    def test_remove_then_rename_into(self, sm):
        """REMOVED /target, rename /src → /target — update not delete."""
        sm.process_events(
            [ev('/target', EventKind.CREATED), ev('/src', EventKind.CREATED)],
        )
        sm.emit()

        sm.process_events([ev('/target', EventKind.REMOVED)])
        sm.process_events(
            [ev('/src', EventKind.RENAMED), ev('/target', EventKind.RENAMED)],
        )

        changes = sm.emit()
        update_paths = {c['path'] for c in changes if c['op'] == 'update'}
        delete_paths = {c['path'] for c in changes if c['op'] == 'delete'}
        assert '/target' in update_paths
        assert '/target' not in delete_paths
        assert '/src' in delete_paths


# ---------------------------------------------------------------------------
# Collision emitted set tracking
# ---------------------------------------------------------------------------


class TestCollisionEmittedTracking:
    def test_collision_emitted_set(self, sm):
        """After collision rename, emitted set correct."""
        sm.process_events(
            [ev('/old', EventKind.CREATED), ev('/target', EventKind.CREATED)],
        )
        sm.emit()

        sm.process_events(
            [ev('/old', EventKind.RENAMED), ev('/target', EventKind.RENAMED)],
        )
        sm.emit()

        assert '/old' not in sm.emitted
        assert '/target' in sm.emitted

    def test_double_collision(self, sm):
        """A→B, C→B double collision — correct emitted state."""
        sm.process_events(
            [
                ev('/A', EventKind.CREATED),
                ev('/B', EventKind.CREATED),
                ev('/C', EventKind.CREATED),
            ],
        )
        sm.emit()

        sm.process_events(
            [ev('/A', EventKind.RENAMED), ev('/B', EventKind.RENAMED)],
        )
        sm.process_events(
            [ev('/C', EventKind.RENAMED), ev('/B', EventKind.RENAMED)],
        )

        assert len(sm.files) == 1
        changes = sm.emit()
        delete_paths = {c['path'] for c in changes if c['op'] == 'delete'}
        assert '/A' in delete_paths
        assert '/C' in delete_paths


# ---------------------------------------------------------------------------
# Idempotent emit
# ---------------------------------------------------------------------------


class TestIdempotentEmit:
    def test_triple_emit_empty(self, sm):
        """3 consecutive emits — only first has content."""
        sm.process_events([ev('/a.txt', EventKind.CREATED)])
        assert len(sm.emit()) == 1
        assert len(sm.emit()) == 0
        assert len(sm.emit()) == 0

    def test_emit_after_delete_no_dup(self, sm):
        """Delete emitted once, not duplicated."""
        sm.process_events([ev('/a.txt', EventKind.CREATED)])
        sm.emit()
        sm.process_events([ev('/a.txt', EventKind.REMOVED)])
        assert len(sm.emit()) == 1
        assert len(sm.emit()) == 0

    def test_sets_cleared_after_emit(self, sm):
        """to_update and to_delete cleared after emit."""
        sm.process_events(
            [ev('/a', EventKind.CREATED), ev('/b', EventKind.CREATED)],
        )
        sm.emit()
        assert len(sm.to_update) == 0
        assert len(sm.to_delete) == 0


# ---------------------------------------------------------------------------
# Cross-parent rename tracking
# ---------------------------------------------------------------------------


class TestCrossParentRename:
    def test_both_parents_updated(self, sm):
        """Rename /dir1/file → /dir2/file — both parents marked for update."""
        sm.process_events(
            [
                ev('/dir1', EventKind.CREATED, is_dir=True),
                ev('/dir2', EventKind.CREATED, is_dir=True),
                ev('/dir1/file.txt', EventKind.CREATED),
            ],
        )
        sm.emit()
        sm.to_update.clear()

        sm.process_events(
            [
                ev('/dir1/file.txt', EventKind.RENAMED),
                ev('/dir2/file.txt', EventKind.RENAMED),
            ],
        )
        assert '/dir1' in sm.to_update
        assert '/dir2' in sm.to_update
        assert sm.files['/dir2/file.txt']['parent'] == '/dir2'

    def test_cross_dir_rename_emitted(self, sm):
        """Cross-dir rename emit — delete old, update new + both parents."""
        sm.process_events(
            [
                ev('/src', EventKind.CREATED, is_dir=True),
                ev('/dst', EventKind.CREATED, is_dir=True),
                ev('/src/data.csv', EventKind.CREATED),
            ],
        )
        sm.emit()

        sm.process_events(
            [
                ev('/src/data.csv', EventKind.RENAMED),
                ev('/dst/data.csv', EventKind.RENAMED),
            ],
        )
        changes = sm.emit()
        update_paths = {c['path'] for c in changes if c['op'] == 'update'}
        delete_paths = {c['path'] for c in changes if c['op'] == 'delete'}
        assert '/src/data.csv' in delete_paths
        assert '/dst/data.csv' in update_paths
        assert '/src' in update_paths
        assert '/dst' in update_paths


# ---------------------------------------------------------------------------
# Long rename chains
# ---------------------------------------------------------------------------


class TestLongRenameChain:
    def test_5_step_rename(self, sm):
        """A→B→C→D→E — file ends at /E."""
        sm.process_events([ev('/A', EventKind.CREATED)])
        for old, new in [
            ('/A', '/B'),
            ('/B', '/C'),
            ('/C', '/D'),
            ('/D', '/E'),
        ]:
            sm.process_events(
                [ev(old, EventKind.RENAMED), ev(new, EventKind.RENAMED)],
            )
        assert '/E' in sm.files
        assert len(sm.files) == 1

    def test_5_step_chain_emitted_deletes(self, sm):
        """5-step chain after emit — all intermediates deleted."""
        sm.process_events([ev('/A', EventKind.CREATED)])
        sm.emit()
        for old, new in [
            ('/A', '/B'),
            ('/B', '/C'),
            ('/C', '/D'),
            ('/D', '/E'),
        ]:
            sm.process_events(
                [ev(old, EventKind.RENAMED), ev(new, EventKind.RENAMED)],
            )
        changes = sm.emit()
        delete_paths = {c['path'] for c in changes if c['op'] == 'delete'}
        assert '/A' in delete_paths
        assert '/E' in {c['path'] for c in changes if c['op'] == 'update'}


# ---------------------------------------------------------------------------
# Deep modify after ancestor rename + emit
# ---------------------------------------------------------------------------


class TestDeepModifyAfterRenameEmit:
    def test_modify_after_rename_and_emit(self, sm):
        """Create tree → emit → rename → emit → modify deep child → emit."""
        sm.process_events(
            [
                ev('/root', EventKind.CREATED, is_dir=True),
                ev('/root/a', EventKind.CREATED, is_dir=True),
                ev('/root/a/b', EventKind.CREATED, is_dir=True),
                ev('/root/a/b/file.txt', EventKind.CREATED),
            ],
        )
        sm.emit()
        sm.process_events(
            [
                ev('/root', EventKind.RENAMED, is_dir=True),
                ev('/newroot', EventKind.RENAMED, is_dir=True),
            ],
        )
        sm.emit()

        sm.process_events([ev('/newroot/a/b/file.txt', EventKind.MODIFIED)])
        changes = sm.emit()
        assert len(changes) == 1
        assert changes[0]['path'] == '/newroot/a/b/file.txt'


# ---------------------------------------------------------------------------
# Multi-level rename — exact children sets
# ---------------------------------------------------------------------------


class TestMultiLevelRenameChildren:
    def test_3_level_exact_children_sets(self, sm):
        """Rename 3-level tree — verify exact children sets at each level."""
        sm.process_events(
            [
                ev('/a', EventKind.CREATED, is_dir=True),
                ev('/a/f1.txt', EventKind.CREATED),
                ev('/a/f2.txt', EventKind.CREATED),
                ev('/a/b', EventKind.CREATED, is_dir=True),
                ev('/a/b/f3.txt', EventKind.CREATED),
                ev('/a/b/c', EventKind.CREATED, is_dir=True),
                ev('/a/b/c/f4.txt', EventKind.CREATED),
                ev('/a/b/c/f5.txt', EventKind.CREATED),
            ],
        )
        sm.process_events(
            [
                ev('/a', EventKind.RENAMED, is_dir=True),
                ev('/z', EventKind.RENAMED, is_dir=True),
            ],
        )

        assert sm.children['/z'] == {'/z/f1.txt', '/z/f2.txt', '/z/b'}
        assert sm.children['/z/b'] == {'/z/b/f3.txt', '/z/b/c'}
        assert sm.children['/z/b/c'] == {'/z/b/c/f4.txt', '/z/b/c/f5.txt'}
        for key in ['/a', '/a/b', '/a/b/c']:
            assert key not in sm.children


# ---------------------------------------------------------------------------
# Rename + recreate old + modify new
# ---------------------------------------------------------------------------


class TestRenameRecreateModify:
    def test_rename_recreate_old_modify_new(self, sm):
        """A→B, create new A, modify B — both exist, no spurious delete."""
        sm.process_events([ev('/A', EventKind.CREATED)])
        sm.emit()

        sm.process_events(
            [ev('/A', EventKind.RENAMED), ev('/B', EventKind.RENAMED)],
        )
        sm.process_events([ev('/A', EventKind.CREATED)])
        sm.process_events([ev('/B', EventKind.MODIFIED)])

        changes = sm.emit()
        update_paths = {c['path'] for c in changes if c['op'] == 'update'}
        delete_paths = {c['path'] for c in changes if c['op'] == 'delete'}
        assert '/A' in update_paths
        assert '/B' in update_paths
        assert '/A' not in delete_paths


# ---------------------------------------------------------------------------
# Multiple independent renames in single batch
# ---------------------------------------------------------------------------


class TestMultiFileRenameBatch:
    def test_three_independent_renames(self, sm):
        """3 independent rename pairs — all tracked correctly."""
        sm.process_events(
            [
                ev('/x1', EventKind.CREATED),
                ev('/x2', EventKind.CREATED),
                ev('/x3', EventKind.CREATED),
            ],
        )
        sm.emit()

        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events(
            [
                ev('/x1', EventKind.RENAMED),
                ev('/y1', EventKind.RENAMED),
                ev('/x2', EventKind.RENAMED),
                ev('/y2', EventKind.RENAMED),
                ev('/x3', EventKind.RENAMED),
                ev('/y3', EventKind.RENAMED),
            ],
        )
        sm.process_events(bp.flush())

        for i in range(1, 4):
            assert f'/y{i}' in sm.files
        changes = sm.emit()
        delete_paths = {c['path'] for c in changes if c['op'] == 'delete'}
        update_paths = {c['path'] for c in changes if c['op'] == 'update'}
        assert {'/x1', '/x2', '/x3'} <= delete_paths
        assert {'/y1', '/y2', '/y3'} <= update_paths


# ---------------------------------------------------------------------------
# Long and special character paths
# ---------------------------------------------------------------------------


class TestLongAndSpecialPaths:
    def test_4096_char_path(self, sm):
        long_path = '/' + 'a' * 4095
        sm.process_events([ev(long_path, EventKind.CREATED)])
        assert long_path in sm.files

    def test_deeply_nested_100_levels(self, sm):
        path = '/' + '/'.join(f'd{i}' for i in range(100))
        sm.process_events([ev(path, EventKind.CREATED)])
        assert path in sm.files

    def test_path_with_spaces(self, sm):
        sm.process_events([ev('/my dir/my file.txt', EventKind.CREATED)])
        assert sm.files['/my dir/my file.txt']['name'] == 'my file.txt'
        assert sm.files['/my dir/my file.txt']['parent'] == '/my dir'

    def test_hidden_files(self, sm):
        sm.process_events([ev('/home/.config/.hidden', EventKind.CREATED)])
        assert sm.files['/home/.config/.hidden']['name'] == '.hidden'

    def test_unicode_path(self, sm):
        sm.process_events([ev('/datos/archivo_日本語.txt', EventKind.CREATED)])
        assert '/datos/archivo_日本語.txt' in sm.files

    def test_rename_with_spaces(self, sm):
        sm.process_events([ev('/old name.txt', EventKind.CREATED)])
        sm.process_events(
            [
                ev('/old name.txt', EventKind.RENAMED),
                ev('/new name.txt', EventKind.RENAMED),
            ],
        )
        assert '/new name.txt' in sm.files


# ---------------------------------------------------------------------------
# Dir removal with modified children
# ---------------------------------------------------------------------------


class TestDirRemovalModifiedChildren:
    def test_modify_then_remove_dir(self, sm):
        """MODIFIED children + REMOVED dir — all cleaned, correct deletes."""
        sm.process_events(
            [
                ev('/dir', EventKind.CREATED, is_dir=True),
                ev('/dir/a.txt', EventKind.CREATED),
                ev('/dir/b.txt', EventKind.CREATED),
            ],
        )
        sm.emit()

        sm.process_events(
            [
                ev('/dir/a.txt', EventKind.MODIFIED),
                ev('/dir/b.txt', EventKind.MODIFIED),
                ev('/dir', EventKind.REMOVED, is_dir=True),
            ],
        )
        changes = sm.emit()
        delete_paths = {c['path'] for c in changes if c['op'] == 'delete'}
        assert {'/dir', '/dir/a.txt', '/dir/b.txt'} == delete_paths
        assert not any(c['op'] == 'update' for c in changes)

    def test_new_child_then_remove_dir(self, sm):
        """New child (never emitted) + remove dir — no delete for new child."""
        sm.process_events(
            [
                ev('/dir', EventKind.CREATED, is_dir=True),
                ev('/dir/old.txt', EventKind.CREATED),
            ],
        )
        sm.emit()

        sm.process_events(
            [
                ev('/dir/new.txt', EventKind.CREATED),
                ev('/dir', EventKind.REMOVED, is_dir=True),
            ],
        )
        changes = sm.emit()
        delete_paths = {c['path'] for c in changes if c['op'] == 'delete'}
        assert '/dir/new.txt' not in delete_paths
        assert '/dir/old.txt' in delete_paths


# ---------------------------------------------------------------------------
# Multiple pending rename flushes
# ---------------------------------------------------------------------------


class TestMultipleRenameFlushes:
    def test_two_sequential_flushes(self, sm):
        """RENAMED(A), MODIFIED flushes A. RENAMED(C), CREATED flushes C."""
        sm.process_events(
            [ev('/a', EventKind.CREATED), ev('/c', EventKind.CREATED)],
        )
        sm.emit()

        sm.process_events(
            [
                ev('/a', EventKind.RENAMED),
                ev('/b', EventKind.MODIFIED),  # flushes /a
                ev('/c', EventKind.RENAMED),
                ev('/d', EventKind.CREATED),  # flushes /c
            ],
        )

        assert '/a' not in sm.files
        assert '/c' not in sm.files
        assert '/d' in sm.files

    def test_flush_then_pair(self, sm):
        """RENAMED(A), MODIFIED flushes A. RENAMED(C), RENAMED(D) pairs C→D."""
        sm.process_events(
            [ev('/a', EventKind.CREATED), ev('/c', EventKind.CREATED)],
        )
        sm.emit()

        sm.process_events(
            [
                ev('/a', EventKind.RENAMED),
                ev('/b', EventKind.MODIFIED),
                ev('/c', EventKind.RENAMED),
                ev('/d', EventKind.RENAMED),
            ],
        )

        assert '/d' in sm.files
        assert '/a' not in sm.files


# ---------------------------------------------------------------------------
# Entry field verification after complex ops
# ---------------------------------------------------------------------------


class TestEntryFieldsComplex:
    def test_all_fields_after_create(self, sm):
        sm.process_events(
            [ev('/test.txt', EventKind.CREATED, ts=42.0, is_symlink=True)],
        )
        entry = sm.files['/test.txt']
        assert entry == {
            'path': '/test.txt',
            'name': 'test.txt',
            'parent': '',
            'is_dir': False,
            'is_symlink': True,
            'ts': 42.0,
        }

    def test_fields_after_rename(self, sm):
        sm.process_events([ev('/dir/old.txt', EventKind.CREATED, ts=1.0)])
        sm.process_events(
            [
                ev('/dir/old.txt', EventKind.RENAMED, ts=2.0),
                ev('/other/new.txt', EventKind.RENAMED, ts=3.0),
            ],
        )
        entry = sm.files['/other/new.txt']
        assert entry['path'] == '/other/new.txt'
        assert entry['name'] == 'new.txt'
        assert entry['parent'] == '/other'
        assert entry['ts'] == 3.0

    def test_is_dir_preserved_through_rename(self, sm):
        sm.process_events([ev('/old', EventKind.CREATED, is_dir=True)])
        sm.process_events(
            [
                ev('/old', EventKind.RENAMED, is_dir=True),
                ev('/new', EventKind.RENAMED, is_dir=True),
            ],
        )
        assert sm.files['/new']['is_dir'] is True


# ---------------------------------------------------------------------------
# Subtree sibling isolation
# ---------------------------------------------------------------------------


class TestSubtreeSiblingIsolation:
    def test_remove_child_dir_sibling_survives(self, sm):
        """Remove /parent/a — /parent/b untouched."""
        sm.process_events(
            [
                ev('/parent', EventKind.CREATED, is_dir=True),
                ev('/parent/a', EventKind.CREATED, is_dir=True),
                ev('/parent/a/deep.txt', EventKind.CREATED),
                ev('/parent/b', EventKind.CREATED, is_dir=True),
                ev('/parent/b/safe.txt', EventKind.CREATED),
            ],
        )
        sm.process_events([ev('/parent/a', EventKind.REMOVED, is_dir=True)])
        assert '/parent/a' not in sm.files
        assert '/parent/b' in sm.files
        assert '/parent/b/safe.txt' in sm.files

    def test_remove_leaf_siblings_survive(self, sm):
        """Remove one file — others survive."""
        sm.process_events(
            [
                ev('/dir', EventKind.CREATED, is_dir=True),
                ev('/dir/a.txt', EventKind.CREATED),
                ev('/dir/b.txt', EventKind.CREATED),
            ],
        )
        sm.process_events([ev('/dir/b.txt', EventKind.REMOVED)])
        assert '/dir/a.txt' in sm.files
        assert len(sm.children['/dir']) == 1


# ---------------------------------------------------------------------------
# State consistency verification
# ---------------------------------------------------------------------------


class TestStateConsistency:
    def test_consistency_after_mixed_ops(self, sm):
        """Create, modify, remove, rename — verify internal state invariants."""
        sm.process_events(
            [
                ev('/d1', EventKind.CREATED, is_dir=True),
                ev('/d2', EventKind.CREATED, is_dir=True),
            ],
        )
        for i in range(5):
            sm.process_events([ev(f'/d1/f{i}', EventKind.CREATED)])
            sm.process_events([ev(f'/d2/f{i}', EventKind.CREATED)])
        sm.emit()

        sm.process_events(
            [ev('/d2/f0', EventKind.REMOVED), ev('/d2/f1', EventKind.REMOVED)],
        )
        sm.process_events(
            [
                ev('/d1', EventKind.RENAMED, is_dir=True),
                ev('/d3', EventKind.RENAMED, is_dir=True),
            ],
        )
        sm.emit()

        for path, entry in sm.files.items():
            expected_parent = path.rpartition('/')[0]
            assert entry['parent'] == expected_parent
        for parent, children in sm.children.items():
            for child in children:
                assert child in sm.files
        assert len(sm.to_update) == 0
        assert len(sm.to_delete) == 0

    def test_consistency_all_deleted(self, sm):
        """Create all → emit → delete all → emit → state empty."""
        for i in range(10):
            sm.process_events([ev(f'/f{i}', EventKind.CREATED)])
        sm.emit()
        for i in range(10):
            sm.process_events([ev(f'/f{i}', EventKind.REMOVED)])
        sm.emit()
        assert len(sm.files) == 0
        assert len(sm.emitted) == 0


# ---------------------------------------------------------------------------
# Rename dir after child removed
# ---------------------------------------------------------------------------


class TestRenameDirAfterChildRemoved:
    def test_removed_child_excluded_from_rename(self, sm):
        """Remove child then rename dir — removed child not recreated."""
        sm.process_events(
            [
                ev('/a', EventKind.CREATED, is_dir=True),
                ev('/a/keep.txt', EventKind.CREATED),
                ev('/a/gone.txt', EventKind.CREATED),
            ],
        )
        sm.emit()

        sm.process_events([ev('/a/gone.txt', EventKind.REMOVED)])
        sm.process_events(
            [
                ev('/a', EventKind.RENAMED, is_dir=True),
                ev('/b', EventKind.RENAMED, is_dir=True),
            ],
        )

        assert '/b/keep.txt' in sm.files
        assert '/b/gone.txt' not in sm.files


# ---------------------------------------------------------------------------
# Emitted set lifecycle
# ---------------------------------------------------------------------------


class TestEmittedSetLifecycle:
    def test_grows_on_emit(self, sm):
        sm.process_events([ev('/a', EventKind.CREATED)])
        sm.emit()
        assert '/a' in sm.emitted

    def test_shrinks_on_delete(self, sm):
        sm.process_events([ev('/a', EventKind.CREATED)])
        sm.emit()
        sm.process_events([ev('/a', EventKind.REMOVED)])
        sm.emit()
        assert '/a' not in sm.emitted

    def test_tracks_renames(self, sm):
        sm.process_events([ev('/a', EventKind.CREATED)])
        sm.emit()
        sm.process_events(
            [ev('/a', EventKind.RENAMED), ev('/b', EventKind.RENAMED)],
        )
        sm.emit()
        assert '/b' in sm.emitted
        assert '/a' not in sm.emitted


# ---------------------------------------------------------------------------
# Root-level operations
# ---------------------------------------------------------------------------


class TestRootLevelOps:
    def test_root_file_parent_not_tracked(self, sm):
        """Create /file.txt — parent '/' not in to_update."""
        sm.process_events([ev('/file.txt', EventKind.CREATED)])
        assert '/' not in sm.to_update

    def test_remove_root_file_parent_not_updated(self, sm):
        """Remove /file.txt — parent '/' not updated."""
        sm.process_events([ev('/file.txt', EventKind.CREATED)])
        sm.emit()
        sm.process_events([ev('/file.txt', EventKind.REMOVED)])
        assert '/' not in sm.to_update


# ---------------------------------------------------------------------------
# Double dir rename (back and forth)
# ---------------------------------------------------------------------------


class TestDoubleDirRename:
    def test_rename_and_back(self, sm):
        """A→B then B→A — state back to original, children get updates."""
        sm.process_events(
            [
                ev('/a', EventKind.CREATED, is_dir=True),
                ev('/a/file.txt', EventKind.CREATED),
            ],
        )
        sm.emit()

        sm.process_events(
            [
                ev('/a', EventKind.RENAMED, is_dir=True),
                ev('/b', EventKind.RENAMED, is_dir=True),
            ],
        )
        sm.process_events(
            [
                ev('/b', EventKind.RENAMED, is_dir=True),
                ev('/a', EventKind.RENAMED, is_dir=True),
            ],
        )

        assert '/a/file.txt' in sm.files
        changes = sm.emit()
        update_paths = {c['path'] for c in changes if c['op'] == 'update'}
        assert '/a' in update_paths
        assert '/a/file.txt' in update_paths

    def test_circular_dir_rename(self, sm):
        """x→y→z→x with children."""
        sm.process_events(
            [
                ev('/x', EventKind.CREATED, is_dir=True),
                ev('/x/f.txt', EventKind.CREATED),
            ],
        )
        sm.process_events(
            [
                ev('/x', EventKind.RENAMED, is_dir=True),
                ev('/y', EventKind.RENAMED, is_dir=True),
            ],
        )
        sm.process_events(
            [
                ev('/y', EventKind.RENAMED, is_dir=True),
                ev('/z', EventKind.RENAMED, is_dir=True),
            ],
        )
        sm.process_events(
            [
                ev('/z', EventKind.RENAMED, is_dir=True),
                ev('/x', EventKind.RENAMED, is_dir=True),
            ],
        )
        assert '/x/f.txt' in sm.files
        assert sm.files['/x/f.txt']['parent'] == '/x'

    def test_deep_rename_back_all_children_updated(self, sm):
        """3-level tree rename-back — ALL children get updates (bug #3 regression)."""
        sm.process_events(
            [
                ev('/a', EventKind.CREATED, is_dir=True),
                ev('/a/L1', EventKind.CREATED, is_dir=True),
                ev('/a/L1/L2', EventKind.CREATED, is_dir=True),
                ev('/a/L1/L2/leaf.txt', EventKind.CREATED),
            ],
        )
        sm.emit()

        sm.process_events(
            [
                ev('/a', EventKind.RENAMED, is_dir=True),
                ev('/b', EventKind.RENAMED, is_dir=True),
            ],
        )
        sm.process_events(
            [
                ev('/b', EventKind.RENAMED, is_dir=True),
                ev('/a', EventKind.RENAMED, is_dir=True),
            ],
        )

        changes = sm.emit()
        update_paths = {c['path'] for c in changes if c['op'] == 'update'}
        assert {'/a', '/a/L1', '/a/L1/L2', '/a/L1/L2/leaf.txt'} <= update_paths


# ---------------------------------------------------------------------------
# Rename over path with removed children
# ---------------------------------------------------------------------------


class TestRenameOverRemovedChildren:
    def test_removed_child_replaced_by_rename(self, sm):
        """REMOVED /b/f.txt, rename /a→/b. /a/f.txt → /b/f.txt replaces."""
        sm.process_events(
            [
                ev('/a', EventKind.CREATED, is_dir=True),
                ev('/a/f.txt', EventKind.CREATED),
                ev('/b', EventKind.CREATED, is_dir=True),
                ev('/b/f.txt', EventKind.CREATED),
            ],
        )
        sm.emit()

        sm.process_events([ev('/b/f.txt', EventKind.REMOVED)])
        sm.process_events([ev('/b', EventKind.REMOVED, is_dir=True)])
        sm.process_events(
            [
                ev('/a', EventKind.RENAMED, is_dir=True),
                ev('/b', EventKind.RENAMED, is_dir=True),
            ],
        )

        assert '/b/f.txt' in sm.files
        changes = sm.emit()
        update_paths = {c['path'] for c in changes if c['op'] == 'update'}
        assert '/b/f.txt' in update_paths


# ---------------------------------------------------------------------------
# Emitted consistency across rename chains
# ---------------------------------------------------------------------------


class TestEmittedConsistencyChain:
    def test_a_b_c_emitted_final_only(self, sm):
        """A→B→C with emit — emitted has only final path."""
        sm.process_events([ev('/A', EventKind.CREATED)])
        sm.emit()
        sm.process_events(
            [ev('/A', EventKind.RENAMED), ev('/B', EventKind.RENAMED)],
        )
        sm.emit()
        sm.process_events(
            [ev('/B', EventKind.RENAMED), ev('/C', EventKind.RENAMED)],
        )
        sm.emit()
        assert '/C' in sm.emitted
        assert '/A' not in sm.emitted
        assert '/B' not in sm.emitted


# ---------------------------------------------------------------------------
# Mixed children after rename (moved + new)
# ---------------------------------------------------------------------------


class TestMixedChildrenAfterRename:
    def test_moved_and_new_children(self, sm):
        """Rename dir then add new files — both tracked."""
        sm.process_events(
            [
                ev('/src', EventKind.CREATED, is_dir=True),
                ev('/src/old.txt', EventKind.CREATED),
            ],
        )
        sm.emit()
        sm.process_events(
            [
                ev('/src', EventKind.RENAMED, is_dir=True),
                ev('/dst', EventKind.RENAMED, is_dir=True),
            ],
        )
        sm.process_events([ev('/dst/new.txt', EventKind.CREATED)])

        assert {'/dst/old.txt', '/dst/new.txt'} <= sm.children['/dst']
        changes = sm.emit()
        update_paths = {c['path'] for c in changes if c['op'] == 'update'}
        assert '/dst/old.txt' in update_paths
        assert '/dst/new.txt' in update_paths


# ---------------------------------------------------------------------------
# Rapid create/remove/emit cycles
# ---------------------------------------------------------------------------


class TestRapidCyclesState:
    def test_100_create_remove_no_deletes(self, sm):
        """100 create/remove cycles with emit — no deletes (never emitted before removal)."""
        total_deletes = 0
        for i in range(100):
            sm.process_events([ev(f'/f_{i}', EventKind.CREATED)])
            sm.process_events([ev(f'/f_{i}', EventKind.REMOVED)])
            total_deletes += sum(1 for c in sm.emit() if c['op'] == 'delete')
        assert total_deletes == 0

    def test_100_create_emit_remove_emit_exact(self, sm):
        """100 create+emit+remove+emit cycles — 100 updates + 100 deletes."""
        total_updates = total_deletes = 0
        for i in range(100):
            sm.process_events([ev(f'/f_{i}', EventKind.CREATED)])
            total_updates += sum(1 for c in sm.emit() if c['op'] == 'update')
            sm.process_events([ev(f'/f_{i}', EventKind.REMOVED)])
            total_deletes += sum(1 for c in sm.emit() if c['op'] == 'delete')
        assert total_updates == 100
        assert total_deletes == 100
        assert len(sm.emitted) == 0


# ---------------------------------------------------------------------------
# Dir overwrite by CREATED (no REMOVED)
# ---------------------------------------------------------------------------


class TestDirOverwriteByCreated:
    def test_dir_with_child_overwritten(self, sm):
        """CREATED dir + child, then CREATED file at same path."""
        sm.process_events(
            [
                ev('/p', EventKind.CREATED, is_dir=True),
                ev('/p/child.txt', EventKind.CREATED),
                ev('/p', EventKind.CREATED, is_dir=False),
            ],
        )
        assert sm.files['/p']['is_dir'] is False
        assert '/p/child.txt' in sm.files


# ---------------------------------------------------------------------------
# Subtree removal idempotency
# ---------------------------------------------------------------------------


class TestSubtreeIdempotency:
    def test_remove_children_individually_then_dir(self, sm):
        """Remove children then dir — all deletes emitted cleanly."""
        sm.process_events(
            [
                ev('/d', EventKind.CREATED, is_dir=True),
                ev('/d/a', EventKind.CREATED),
                ev('/d/b', EventKind.CREATED),
            ],
        )
        sm.emit()
        sm.process_events([ev('/d/a', EventKind.REMOVED)])
        sm.process_events([ev('/d/b', EventKind.REMOVED)])
        sm.process_events([ev('/d', EventKind.REMOVED, is_dir=True)])
        changes = sm.emit()
        assert {c['path'] for c in changes if c['op'] == 'delete'} == {
            '/d',
            '/d/a',
            '/d/b',
        }


# ---------------------------------------------------------------------------
# Stat failure across emit cycles
# ---------------------------------------------------------------------------


class TestStatFailureMultiCycle:
    def test_fail_then_modify_then_succeed(self):
        """Stat fails, MODIFIED re-queues, stat succeeds."""
        behavior = {'fail': True}

        def _stat(p):
            if behavior['fail']:
                raise OSError('fail')
            return make_stat_result()

        sm = StateManager(stat_fn=_stat)
        sm.process_events([ev('/f', EventKind.CREATED)])
        sm.emit()
        behavior['fail'] = False
        sm.process_events([ev('/f', EventKind.MODIFIED)])
        assert len(sm.emit()) == 1

    def test_never_emitted_then_removed_no_delete(self):
        """Stat always fails, then REMOVED — no delete."""
        behavior = {'fail': True}

        def _stat(p):
            if behavior['fail']:
                raise OSError('fail')
            return make_stat_result()

        sm = StateManager(stat_fn=_stat)
        sm.process_events([ev('/f', EventKind.CREATED)])
        for _ in range(3):
            sm.emit()
            sm.process_events([ev('/f', EventKind.MODIFIED)])
        sm.process_events([ev('/f', EventKind.REMOVED)])
        behavior['fail'] = False
        assert not any(c['op'] == 'delete' for c in sm.emit())


# ---------------------------------------------------------------------------
# Children dict behavior
# ---------------------------------------------------------------------------


class TestChildrenDictBehavior:
    def test_defaultdict_creates_empty_set(self, sm):
        """Accessing untracked parent creates empty set (defaultdict)."""
        assert sm.children['/nonexistent'] == set()

    def test_empty_after_all_removed(self, sm):
        """Removing all children leaves empty set."""
        sm.process_events(
            [
                ev('/d', EventKind.CREATED, is_dir=True),
                ev('/d/a', EventKind.CREATED),
                ev('/d/b', EventKind.CREATED),
            ],
        )
        sm.process_events(
            [ev('/d/a', EventKind.REMOVED), ev('/d/b', EventKind.REMOVED)],
        )
        assert len(sm.children['/d']) == 0


# ---------------------------------------------------------------------------
# Rename never-emitted dir
# ---------------------------------------------------------------------------


class TestRenameNeverEmittedDir:
    def test_no_deletes_for_unemitted(self, sm):
        """Rename dir that was never emitted — no deletes."""
        sm.process_events(
            [
                ev('/new', EventKind.CREATED, is_dir=True),
                ev('/new/f.txt', EventKind.CREATED),
            ],
        )
        sm.process_events(
            [
                ev('/new', EventKind.RENAMED, is_dir=True),
                ev('/final', EventKind.RENAMED, is_dir=True),
            ],
        )
        changes = sm.emit()
        assert not any(c['op'] == 'delete' for c in changes)
        assert any(c['path'] == '/final' for c in changes)

    def test_partially_emitted_dir(self, sm):
        """Dir emitted, child added after, then rename.
        Dir gets delete, late child doesn't.
        """
        sm.process_events([ev('/d', EventKind.CREATED, is_dir=True)])
        sm.emit()
        sm.process_events([ev('/d/late.txt', EventKind.CREATED)])
        sm.process_events(
            [
                ev('/d', EventKind.RENAMED, is_dir=True),
                ev('/e', EventKind.RENAMED, is_dir=True),
            ],
        )
        changes = sm.emit()
        delete_paths = {c['path'] for c in changes if c['op'] == 'delete'}
        assert '/d' in delete_paths
        assert '/d/late.txt' not in delete_paths


# ---------------------------------------------------------------------------
# Emit with only deletes
# ---------------------------------------------------------------------------


class TestEmitOnlyDeletes:
    def test_all_removed_only_deletes(self, sm):
        """All files removed — emit produces only deletes."""
        sm.process_events(
            [ev('/a', EventKind.CREATED), ev('/b', EventKind.CREATED)],
        )
        sm.emit()
        sm.process_events(
            [ev('/a', EventKind.REMOVED), ev('/b', EventKind.REMOVED)],
        )
        changes = sm.emit()
        assert len(changes) == 2
        assert all(c['op'] == 'delete' for c in changes)


# ---------------------------------------------------------------------------
# Unpaired dir rename with children cascade
# ---------------------------------------------------------------------------


class TestUnpairedDirRenameWithChildren:
    def test_unpaired_dir_rename_cascades(self, sm):
        """Unpaired dir RENAMED — children cascade-removed after grace."""
        sm.process_events(
            [
                ev('/dir', EventKind.CREATED, is_dir=True),
                ev('/dir/a.txt', EventKind.CREATED),
                ev('/dir/sub', EventKind.CREATED, is_dir=True),
                ev('/dir/sub/b.txt', EventKind.CREATED),
            ],
        )
        sm.emit()
        sm.process_events([ev('/dir', EventKind.RENAMED, is_dir=True)])
        sm.emit()  # grace cycle
        changes = sm.emit()  # flushed as removal
        delete_paths = {c['path'] for c in changes if c['op'] == 'delete'}
        assert {
            '/dir',
            '/dir/a.txt',
            '/dir/sub',
            '/dir/sub/b.txt',
        } <= delete_paths

    def test_flushed_by_non_rename(self, sm):
        """Unpaired dir RENAMED flushed by CREATED — children removed."""
        sm.process_events(
            [
                ev('/d', EventKind.CREATED, is_dir=True),
                ev('/d/f.txt', EventKind.CREATED),
            ],
        )
        sm.emit()
        sm.process_events(
            [
                ev('/d', EventKind.RENAMED, is_dir=True),
                ev('/other.txt', EventKind.CREATED),
            ],
        )
        assert '/d' not in sm.files
        assert '/d/f.txt' not in sm.files


# ---------------------------------------------------------------------------
# Multiple events same path in one process_events
# ---------------------------------------------------------------------------


class TestMultiEventSamePathOneCall:
    def test_create_modify_remove(self, sm):
        sm.process_events(
            [
                ev('/f', EventKind.CREATED),
                ev('/f', EventKind.MODIFIED),
                ev('/f', EventKind.REMOVED),
            ],
        )
        assert '/f' not in sm.files

    def test_create_rename_create(self, sm):
        sm.process_events(
            [
                ev('/a', EventKind.CREATED),
                ev('/a', EventKind.RENAMED),
                ev('/b', EventKind.RENAMED),
                ev('/a', EventKind.CREATED),
            ],
        )
        assert '/a' in sm.files
        assert '/b' in sm.files
