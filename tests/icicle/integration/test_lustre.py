"""Integration tests for Lustre changelog monitor against live /mnt/fs0.

These tests require a mounted Lustre filesystem at /mnt/fs0 with
changelog enabled on at least one MDT.  Every test is automatically
parameterized across all MDTs that have changelog registered.
Skipped automatically when /mnt/fs0 is not mounted.

Run with:  pytest tests/icicle/integration/test_lustre.py -v
"""

from __future__ import annotations

import os
import shutil
import stat
import subprocess
import time
import uuid
from typing import Any

import pytest

from src.icicle.batch import BatchProcessor
from src.icicle.events import EventKind
from src.icicle.fswatch_events import STAT_REDUCTION_RULES
from src.icicle.lustre_events import parse_changelog_line
from src.icicle.lustre_source import LustreChangelogSource
from src.icicle.monitor import Monitor
from src.icicle.state import StateManager

# ---------------------------------------------------------------------------
# Skip if Lustre is not available
# ---------------------------------------------------------------------------
LUSTRE_MOUNT = '/mnt/fs0'
MDT = 'fs0-MDT0000'  # patched per-test by the mdt fixture below
FSNAME = 'fs0'

_lustre_available = os.path.ismount(LUSTRE_MOUNT)
pytestmark = pytest.mark.lustre
skip_no_lustre = pytest.mark.skipif(
    not _lustre_available,
    reason=f'{LUSTRE_MOUNT} not mounted',
)


def _mdt_has_changelog(mdt_name: str) -> bool:
    """Return True if the MDT has a registered changelog user."""
    try:
        r = subprocess.run(
            ['sudo', 'lfs', 'changelog', mdt_name, '0'],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
        return r.returncode == 0
    except Exception:
        return False


# Detect which MDTs have changelog enabled at import time.
_ALL_MDTS = ['fs0-MDT0000', 'fs0-MDT0001']
_AVAILABLE_MDTS = (
    [m for m in _ALL_MDTS if _mdt_has_changelog(m)]
    if _lustre_available
    else []
) or ['fs0-MDT0000']  # fallback so collection doesn't crash


@pytest.fixture(
    autouse=True,
    params=_AVAILABLE_MDTS,
    ids=lambda m: m.split('-')[-1],
)
def mdt(request, monkeypatch):
    """Parameterize every test across all MDTs with changelog enabled."""
    monkeypatch.setattr(
        'tests.icicle.integration.test_lustre.MDT',
        request.param,
    )
    return request.param


def _unique_dir() -> str:
    """Create a unique test directory on the current MDT."""
    name = f'icicle_test_{uuid.uuid4().hex[:8]}'
    path = os.path.join(LUSTRE_MOUNT, name)
    mdt_index = int(MDT.split('MDT')[-1], 16)
    subprocess.run(
        ['sudo', 'lfs', 'mkdir', '-i', str(mdt_index), path],
        check=True,
        capture_output=True,
    )
    subprocess.run(
        ['sudo', 'chown', f'{os.getuid()}:{os.getgid()}', path],
        check=True,
        capture_output=True,
    )
    return path


def _get_last_eid() -> int:
    """Get the latest changelog record ID."""
    result = subprocess.run(
        ['sudo', 'lfs', 'changelog', MDT],
        capture_output=True,
        text=True,
        check=True,
    )
    lines = result.stdout.strip().splitlines()
    if not lines:
        return 0
    last = lines[-1].split()[0]
    return int(last)


def _read_changelog_from(startrec: int) -> list[str]:
    """Read changelog lines from a given record."""
    result = subprocess.run(
        ['sudo', 'lfs', 'changelog', MDT, str(startrec)],
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip().splitlines()


class _CapturingOutput:
    """Output handler that captures payloads for assertions."""

    def __init__(self) -> None:
        self.payloads: list[dict[str, Any]] = []

    def send(self, payloads: list[dict[str, Any]]) -> None:
        self.payloads.extend(payloads)

    def close(self) -> None:
        pass


# ===================================================================
# Shared workload tests
# ===================================================================
from tests.icicle.workloads import assert_workload_results
from tests.icicle.workloads import execute_workload
from tests.icicle.workloads import WORKLOADS

WORKLOAD_FINAL_WAIT = 2.0


@pytest.fixture
def harness(mdt):
    """Create test directory and pipeline for workload tests."""
    test_dir = _unique_dir()
    startrec = _get_last_eid()

    def collect(final_wait=WORKLOAD_FINAL_WAIT):
        time.sleep(final_wait)
        _events, changes = _source_and_pipeline(startrec)
        return changes

    yield str(test_dir), collect
    shutil.rmtree(str(test_dir), ignore_errors=True)


@pytest.mark.parametrize('workload', WORKLOADS, ids=lambda w: w.name)
def test_workload(workload, harness):
    """Run a shared workload against Lustre backend."""
    watch_dir, collect = harness
    execute_workload(watch_dir, workload.ops)
    payloads = collect(WORKLOAD_FINAL_WAIT)
    assert_workload_results(payloads, workload.expected)


# ===================================================================
# Changelog parsing (live data)
# ===================================================================
@skip_no_lustre
class TestChangelogParsing:
    def test_parse_existing_changelog(self) -> None:
        """All existing changelog lines should parse without error."""
        lines = _read_changelog_from(0)
        parsed = 0
        for line in lines:
            ev = parse_changelog_line(line)
            if ev is not None:
                parsed += 1
                assert 'eid' in ev
                assert 'lustre_type' in ev
        # We should have at least some parseable events.
        assert parsed > 0

    def test_generate_and_parse_creat(self) -> None:
        """Create a file, read changelog, verify CREAT parsed."""
        test_dir = _unique_dir()
        try:
            startrec = _get_last_eid() + 1
            filepath = os.path.join(test_dir, 'parse_test.txt')
            with open(filepath, 'w') as f:
                f.write('hello')

            time.sleep(0.5)
            lines = _read_changelog_from(startrec)
            creat_events = []
            for line in lines:
                ev = parse_changelog_line(line)
                if ev and ev['lustre_type'].value == '01CREAT':
                    creat_events.append(ev)

            assert len(creat_events) >= 1
            names = [e['name'] for e in creat_events]
            assert 'parse_test.txt' in names
        finally:
            shutil.rmtree(test_dir, ignore_errors=True)


# ===================================================================
# Source event resolution
# ===================================================================
@skip_no_lustre
class TestSourceResolution:
    def test_create_file_resolves_path(self) -> None:
        """Source emits CREATED with correct path for new file."""
        test_dir = _unique_dir()
        try:
            startrec = _get_last_eid() + 1
            filepath = os.path.join(test_dir, 'resolve.txt')
            with open(filepath, 'w') as f:
                f.write('data')

            time.sleep(0.5)
            src = LustreChangelogSource(
                mdt=MDT,
                mount_point=LUSTRE_MOUNT,
                fsname=FSNAME,
                poll_interval=0.0,
                startrec=startrec,
            )
            events = src.read()
            src.close()

            created = [e for e in events if e['kind'] == EventKind.CREATED]
            paths = [e['path'] for e in created]
            assert filepath in paths
        finally:
            shutil.rmtree(test_dir, ignore_errors=True)

    def test_create_dir_and_nested_file(self) -> None:
        """Source resolves nested paths from FID cache."""
        startrec = _get_last_eid() + 1
        test_dir = _unique_dir()
        try:
            subdir = os.path.join(test_dir, 'sub')
            os.makedirs(subdir)
            filepath = os.path.join(subdir, 'nested.txt')
            with open(filepath, 'w') as f:
                f.write('nested')

            time.sleep(0.5)
            src = LustreChangelogSource(
                mdt=MDT,
                mount_point=LUSTRE_MOUNT,
                fsname=FSNAME,
                poll_interval=0.0,
                startrec=startrec,
            )
            events = src.read()
            src.close()

            created_paths = [
                e['path'] for e in events if e['kind'] == EventKind.CREATED
            ]
            assert subdir in created_paths
            assert filepath in created_paths
        finally:
            shutil.rmtree(test_dir, ignore_errors=True)

    def test_rename_produces_paired_events(self) -> None:
        """Source emits two RENAMED events for a file rename."""
        test_dir = _unique_dir()
        try:
            old_path = os.path.join(test_dir, 'before.txt')
            new_path = os.path.join(test_dir, 'after.txt')
            with open(old_path, 'w') as f:
                f.write('rename me')
            time.sleep(0.3)

            startrec = _get_last_eid() + 1
            os.rename(old_path, new_path)
            time.sleep(0.5)

            src = LustreChangelogSource(
                mdt=MDT,
                mount_point=LUSTRE_MOUNT,
                fsname=FSNAME,
                poll_interval=0.0,
                startrec=startrec,
            )
            events = src.read()
            src.close()

            renamed = [e for e in events if e['kind'] == EventKind.RENAMED]
            assert len(renamed) == 2
            assert renamed[0]['path'] == old_path
            assert renamed[1]['path'] == new_path
        finally:
            shutil.rmtree(test_dir, ignore_errors=True)

    def test_delete_resolves_path(self) -> None:
        """Source emits REMOVED with correct path."""
        test_dir = _unique_dir()
        try:
            filepath = os.path.join(test_dir, 'to_delete.txt')
            with open(filepath, 'w') as f:
                f.write('delete me')
            time.sleep(0.3)

            startrec = _get_last_eid() + 1
            os.remove(filepath)
            time.sleep(0.5)

            src = LustreChangelogSource(
                mdt=MDT,
                mount_point=LUSTRE_MOUNT,
                fsname=FSNAME,
                poll_interval=0.0,
                startrec=startrec,
            )
            events = src.read()
            src.close()

            removed = [e for e in events if e['kind'] == EventKind.REMOVED]
            paths = [e['path'] for e in removed]
            assert filepath in paths
        finally:
            shutil.rmtree(test_dir, ignore_errors=True)

    def test_fid2path_fallback_for_preexisting(self) -> None:
        """Pre-existing file (not in FID cache) resolved via fid2path."""
        test_dir = _unique_dir()
        try:
            filepath = os.path.join(test_dir, 'preexisting.txt')
            with open(filepath, 'w') as f:
                f.write('I exist already')
            time.sleep(0.3)

            # Start reading AFTER the file was created (CREAT not in our window).
            startrec = _get_last_eid() + 1
            # Modify the file to generate a CLOSE/MTIME event.
            with open(filepath, 'a') as f:
                f.write(' appended')
            time.sleep(0.5)

            src = LustreChangelogSource(
                mdt=MDT,
                mount_point=LUSTRE_MOUNT,
                fsname=FSNAME,
                poll_interval=0.0,
                startrec=startrec,
            )
            events = src.read()
            src.close()

            # Should see MODIFIED event for the file via fid2path fallback.
            modified = [e for e in events if e['kind'] == EventKind.MODIFIED]
            paths = [e['path'] for e in modified]
            assert filepath in paths
        finally:
            shutil.rmtree(test_dir, ignore_errors=True)


# ===================================================================
# Full pipeline tests (Lustre-specific lifecycle)
# ===================================================================
@skip_no_lustre
class TestFullPipeline:
    def _run_pipeline(
        self,
        startrec: int,
    ) -> list[dict[str, Any]]:
        """Run source → batch → state → emit pipeline."""
        src = LustreChangelogSource(
            mdt=MDT,
            mount_point=LUSTRE_MOUNT,
            fsname=FSNAME,
            poll_interval=0.0,
            startrec=startrec,
        )
        events = src.read()
        src.close()

        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events(events)
        coalesced = bp.flush()

        sm = StateManager()
        if coalesced:
            sm.process_events(coalesced)
        return sm.emit()

    def test_t5_full_lifecycle_with_delete(self) -> None:
        """T5: Full lifecycle with recursive delete → clean state."""
        test_dir = _unique_dir()
        try:
            startrec = _get_last_eid() + 1
            subdir = os.path.join(test_dir, 'lifecycle')
            os.makedirs(subdir)
            f1 = os.path.join(subdir, 'f1.txt')
            with open(f1, 'w') as f:
                f.write('will be deleted')
            shutil.rmtree(subdir)

            time.sleep(0.5)
            changes = self._run_pipeline(startrec)

            # After create + delete, the file should have both create and
            # delete ops, but batch reduction may cancel create+delete pairs.
            # At minimum, verify no orphan updates remain.
            final_updates = {c['path'] for c in changes if c['op'] == 'update'}
            assert subdir not in final_updates
            assert f1 not in final_updates
        finally:
            shutil.rmtree(test_dir, ignore_errors=True)


# ===================================================================
# Monitor end-to-end
# ===================================================================
@skip_no_lustre
class TestMonitorEndToEnd:
    def test_monitor_captures_events(self) -> None:
        """Full Monitor pipeline with LustreChangelogSource."""
        test_dir = _unique_dir()
        try:
            startrec = _get_last_eid() + 1
            filepath = os.path.join(test_dir, 'monitor_test.txt')
            with open(filepath, 'w') as f:
                f.write('monitor test')

            time.sleep(0.5)
            src = LustreChangelogSource(
                mdt=MDT,
                mount_point=LUSTRE_MOUNT,
                fsname=FSNAME,
                poll_interval=0.0,
                startrec=startrec,
            )
            output = _CapturingOutput()
            monitor = Monitor(src, output)

            # Read once (source returns events, then empty on next read → exits).
            events = src.read()
            if events:
                monitor.batch.add_events(events)
                coalesced = monitor.batch.flush()
                if coalesced:
                    monitor.state.process_events(coalesced)
                    updates = monitor.state.emit()
                    if updates:
                        output.send(updates)
            src.close()

            update_paths = {
                c['path'] for c in output.payloads if c['op'] == 'update'
            }
            assert filepath in update_paths
        finally:
            shutil.rmtree(test_dir, ignore_errors=True)


# ===================================================================
# Helper for pipeline tests below
# ===================================================================
def _source_and_pipeline(
    startrec: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Read events and run pipeline, return (raw_events, changes)."""
    src = LustreChangelogSource(
        mdt=MDT,
        mount_point=LUSTRE_MOUNT,
        fsname=FSNAME,
        poll_interval=0.0,
        startrec=startrec,
    )
    events = src.read()
    src.close()

    bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
    bp.add_events(events)
    coalesced = bp.flush()

    sm = StateManager()
    if coalesced:
        sm.process_events(coalesced)
    changes = sm.emit()
    return events, changes


# ===================================================================
# Unknown event types (HLINK, SLINK, etc.)
# ===================================================================
@skip_no_lustre
class TestUnknownEventTypes:
    def test_hard_link_gracefully_skipped(self) -> None:
        """03HLINK is not in LustreEventType — should be skipped."""
        test_dir = _unique_dir()
        try:
            startrec = _get_last_eid() + 1
            original = os.path.join(test_dir, 'original.txt')
            hardlink = os.path.join(test_dir, 'hardlink.txt')
            with open(original, 'w') as f:
                f.write('link target')
            os.link(original, hardlink)

            time.sleep(0.5)
            events, changes = _source_and_pipeline(startrec)

            # CREAT for original should be present.
            created = [e for e in events if e['kind'] == EventKind.CREATED]
            assert any(e['path'] == original for e in created)
            # HLINK event is skipped — no error raised.
            # The hardlink file may not appear as CREATED since HLINK != CREAT.
        finally:
            shutil.rmtree(test_dir, ignore_errors=True)

    def test_symlink_gracefully_skipped(self) -> None:
        """04SLINK is not in LustreEventType — should be skipped."""
        test_dir = _unique_dir()
        try:
            startrec = _get_last_eid() + 1
            target = os.path.join(test_dir, 'target.txt')
            with open(target, 'w') as f:
                f.write('symlink target')
            symlink = os.path.join(test_dir, 'link')
            os.symlink(target, symlink)

            time.sleep(0.5)
            events, changes = _source_and_pipeline(startrec)

            # CREAT for target should be present.
            created = [e for e in events if e['kind'] == EventKind.CREATED]
            assert any(e['path'] == target for e in created)
            # SLINK event is skipped — no error raised.
        finally:
            shutil.rmtree(test_dir, ignore_errors=True)

    def test_pipeline_survives_unknown_events(self) -> None:
        """Mixed known + unknown events don't break the pipeline."""
        test_dir = _unique_dir()
        try:
            startrec = _get_last_eid() + 1
            f1 = os.path.join(test_dir, 'known.txt')
            with open(f1, 'w') as f:
                f.write('known')
            # Generate SLINK (unknown to our enum).
            sym = os.path.join(test_dir, 'sym')
            os.symlink(f1, sym)
            f2 = os.path.join(test_dir, 'also_known.txt')
            with open(f2, 'w') as f:
                f.write('also known')

            time.sleep(0.5)
            events, changes = _source_and_pipeline(startrec)

            update_paths = {c['path'] for c in changes if c['op'] == 'update'}
            assert f1 in update_paths
            assert f2 in update_paths
        finally:
            shutil.rmtree(test_dir, ignore_errors=True)


# ===================================================================
# Rename overwriting existing file
# ===================================================================
@skip_no_lustre
class TestRenameOverwrite:
    def test_rename_overwrites_target(self) -> None:
        """Mv src dst where dst exists → target overwritten."""
        test_dir = _unique_dir()
        try:
            startrec = _get_last_eid() + 1
            dst = os.path.join(test_dir, 'dst.txt')
            src_file = os.path.join(test_dir, 'src.txt')
            with open(dst, 'w') as f:
                f.write('old content')
            with open(src_file, 'w') as f:
                f.write('new content')
            os.rename(src_file, dst)

            time.sleep(0.5)
            events, changes = _source_and_pipeline(startrec)

            update_paths = {c['path'] for c in changes if c['op'] == 'update'}
            # dst should be updated (with new content from src).
            assert dst in update_paths
        finally:
            shutil.rmtree(test_dir, ignore_errors=True)

    def test_rename_overwrite_stat_reflects_source(self) -> None:
        """After mv src dst, stat of dst reflects src's content."""
        test_dir = _unique_dir()
        try:
            startrec = _get_last_eid() + 1
            dst = os.path.join(test_dir, 'dst.txt')
            src_file = os.path.join(test_dir, 'src.txt')
            with open(dst, 'w') as f:
                f.write('short')
            with open(src_file, 'w') as f:
                f.write('much longer content here')
            os.rename(src_file, dst)

            time.sleep(0.5)
            _, changes = _source_and_pipeline(startrec)

            dst_updates = [
                c for c in changes if c['op'] == 'update' and c['path'] == dst
            ]
            assert len(dst_updates) >= 1
            # Stat should reflect the new (longer) content.
            assert dst_updates[-1]['stat']['size'] == len(
                'much longer content here',
            )
        finally:
            shutil.rmtree(test_dir, ignore_errors=True)


# ===================================================================
# Move subtree to new location
# ===================================================================
@skip_no_lustre
class TestMoveSubtree:
    def test_move_populated_dir_across_dirs(self) -> None:
        """Move dir with children from one parent to another."""
        startrec = _get_last_eid() + 1
        test_dir = _unique_dir()
        try:
            parent_a = os.path.join(test_dir, 'a')
            parent_b = os.path.join(test_dir, 'b')
            os.makedirs(parent_a)
            os.makedirs(parent_b)
            sub = os.path.join(parent_a, 'sub')
            os.makedirs(sub)
            f1 = os.path.join(sub, 'file.txt')
            with open(f1, 'w') as f:
                f.write('moving')

            moved_sub = os.path.join(parent_b, 'sub')
            os.rename(sub, moved_sub)

            time.sleep(0.5)
            _, changes = _source_and_pipeline(startrec)

            update_paths = {
                ch['path'] for ch in changes if ch['op'] == 'update'
            }
            assert moved_sub in update_paths
            assert os.path.join(moved_sub, 'file.txt') in update_paths
        finally:
            shutil.rmtree(test_dir, ignore_errors=True)

    def test_move_dir_then_create_inside(self) -> None:
        """Move dir, then create new file inside moved dir."""
        startrec = _get_last_eid() + 1
        test_dir = _unique_dir()
        try:
            src_dir = os.path.join(test_dir, 'src')
            dst_dir = os.path.join(test_dir, 'dst')
            os.makedirs(src_dir)
            new_name = os.path.join(test_dir, 'moved')
            os.rename(src_dir, new_name)
            new_file = os.path.join(new_name, 'new.txt')
            with open(new_file, 'w') as f:
                f.write('created after move')

            time.sleep(0.5)
            _, changes = _source_and_pipeline(startrec)

            update_paths = {
                ch['path'] for ch in changes if ch['op'] == 'update'
            }
            assert new_file in update_paths
        finally:
            shutil.rmtree(test_dir, ignore_errors=True)


# ===================================================================
# Empty directory operations
# ===================================================================
@skip_no_lustre
class TestEmptyDirectoryOps:
    def test_rename_empty_dir(self) -> None:
        """Rename empty directory."""
        startrec = _get_last_eid() + 1
        test_dir = _unique_dir()
        try:
            old = os.path.join(test_dir, 'old_dir')
            new = os.path.join(test_dir, 'new_dir')
            os.makedirs(old)
            os.rename(old, new)

            time.sleep(0.5)
            _, changes = _source_and_pipeline(startrec)

            update_paths = {
                ch['path'] for ch in changes if ch['op'] == 'update'
            }
            assert new in update_paths
        finally:
            shutil.rmtree(test_dir, ignore_errors=True)


# ===================================================================
# OPEN event filtering verification
# ===================================================================
@skip_no_lustre
class TestOpenFiltering:
    def test_no_open_events_in_source_output(self) -> None:
        """Verify no events from OPEN changelog records leak through."""
        test_dir = _unique_dir()
        try:
            startrec = _get_last_eid() + 1
            filepath = os.path.join(test_dir, 'open_test.txt')
            # Multiple opens: write, read, write, read.
            with open(filepath, 'w') as f:
                f.write('write1')
            with open(filepath) as f:
                f.read()
            with open(filepath, 'a') as f:
                f.write('write2')
            with open(filepath) as f:
                f.read()

            time.sleep(0.5)
            # Verify via raw changelog that OPEN events exist.
            lines = _read_changelog_from(startrec)
            open_lines = [l for l in lines if '10OPEN' in l]
            assert len(open_lines) >= 2, (
                'Expected OPEN events in raw changelog'
            )

            # But source should not emit them.
            src = LustreChangelogSource(
                mdt=MDT,
                mount_point=LUSTRE_MOUNT,
                fsname=FSNAME,
                poll_interval=0.0,
                startrec=startrec,
            )
            events = src.read()
            src.close()

            # No event should have come from an OPEN record.
            # All events should be CREATED or MODIFIED (from CLOSE/MTIME).
            for e in events:
                assert e['kind'] in (
                    EventKind.CREATED,
                    EventKind.MODIFIED,
                    EventKind.MODIFIED,
                ), f'Unexpected event kind: {e["kind"]}'
        finally:
            shutil.rmtree(test_dir, ignore_errors=True)


# ===================================================================
# Incremental reads
# ===================================================================
@skip_no_lustre
class TestIncrementalReads:
    def test_two_read_calls_no_duplicates(self) -> None:
        """Two sequential read() calls should not duplicate events."""
        test_dir = _unique_dir()
        try:
            # First batch of operations.
            startrec = _get_last_eid() + 1
            f1 = os.path.join(test_dir, 'batch1.txt')
            with open(f1, 'w') as f:
                f.write('batch1')
            time.sleep(0.5)

            src = LustreChangelogSource(
                mdt=MDT,
                mount_point=LUSTRE_MOUNT,
                fsname=FSNAME,
                poll_interval=0.0,
                startrec=startrec,
            )
            events1 = src.read()

            # Second batch of operations.
            f2 = os.path.join(test_dir, 'batch2.txt')
            with open(f2, 'w') as f:
                f.write('batch2')
            time.sleep(0.5)

            events2 = src.read()
            src.close()

            paths1 = {
                e['path'] for e in events1 if e['kind'] == EventKind.CREATED
            }
            paths2 = {
                e['path'] for e in events2 if e['kind'] == EventKind.CREATED
            }

            assert f1 in paths1
            assert f2 in paths2
            # No overlap.
            assert f2 not in paths1
            assert f1 not in paths2
        finally:
            shutil.rmtree(test_dir, ignore_errors=True)

    def test_startrec_advances_between_reads(self) -> None:
        """Source startrec advances correctly across reads."""
        test_dir = _unique_dir()
        try:
            startrec = _get_last_eid() + 1
            f1 = os.path.join(test_dir, 'inc1.txt')
            with open(f1, 'w') as f:
                f.write('inc1')
            time.sleep(0.5)

            src = LustreChangelogSource(
                mdt=MDT,
                mount_point=LUSTRE_MOUNT,
                fsname=FSNAME,
                poll_interval=0.0,
                startrec=startrec,
            )
            src.read()
            startrec_after_first = src._startrec

            f2 = os.path.join(test_dir, 'inc2.txt')
            with open(f2, 'w') as f:
                f.write('inc2')
            time.sleep(0.5)

            src.read()
            startrec_after_second = src._startrec
            src.close()

            assert startrec_after_second > startrec_after_first
        finally:
            shutil.rmtree(test_dir, ignore_errors=True)


# ===================================================================
# Many files in one directory
# ===================================================================
@skip_no_lustre
class TestManyFiles:
    def test_create_20_files(self) -> None:
        """Create 20 files, verify all detected."""
        startrec = _get_last_eid() + 1
        test_dir = _unique_dir()
        try:
            files = []
            for i in range(20):
                p = os.path.join(test_dir, f'file_{i:03d}.txt')
                with open(p, 'w') as f:
                    f.write(f'content {i}')
                files.append(p)

            time.sleep(0.5)
            _, changes = _source_and_pipeline(startrec)

            update_paths = {
                ch['path'] for ch in changes if ch['op'] == 'update'
            }
            for p in files:
                assert p in update_paths, f'Missing: {p}'
        finally:
            shutil.rmtree(test_dir, ignore_errors=True)

    def test_create_files_in_multiple_dirs(self) -> None:
        """Create files across 5 directories, verify isolation."""
        startrec = _get_last_eid() + 1
        test_dir = _unique_dir()
        try:
            expected = []
            for d in range(5):
                dpath = os.path.join(test_dir, f'dir_{d}')
                os.makedirs(dpath)
                for i in range(3):
                    fpath = os.path.join(dpath, f'f_{i}.txt')
                    with open(fpath, 'w') as f:
                        f.write(f'{d}-{i}')
                    expected.append(fpath)

            time.sleep(0.5)
            _, changes = _source_and_pipeline(startrec)

            update_paths = {
                ch['path'] for ch in changes if ch['op'] == 'update'
            }
            for p in expected:
                assert p in update_paths, f'Missing: {p}'
        finally:
            shutil.rmtree(test_dir, ignore_errors=True)


# ===================================================================
# Event reduction counts
# ===================================================================
@skip_no_lustre
class TestReductionCounts:
    def test_reduction_ratio(self) -> None:
        """Verify batch processor reduces event count significantly."""
        test_dir = _unique_dir()
        try:
            startrec = _get_last_eid() + 1
            filepath = os.path.join(test_dir, 'reduce.txt')
            # Create + 10 writes → many CLOSE/MTIME events.
            for i in range(10):
                with open(filepath, 'w') as f:
                    f.write(f'write {i}')

            time.sleep(0.5)
            src = LustreChangelogSource(
                mdt=MDT,
                mount_point=LUSTRE_MOUNT,
                fsname=FSNAME,
                poll_interval=0.0,
                startrec=startrec,
            )
            events = src.read()
            src.close()

            bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
            bp.add_events(events)

            # Before reduction: many events.
            assert bp.received > 5
            # After reduction: significant reduction.
            assert bp.reduced > 0
            assert bp.accepted < bp.received

            coalesced = bp.flush()
            # Coalesced should have far fewer events than received.
            assert len(coalesced) < bp.received
        finally:
            shutil.rmtree(test_dir, ignore_errors=True)


# ===================================================================
# FID cache correctness
# ===================================================================
@skip_no_lustre
class TestFidCacheCorrectness:
    def test_cache_tracks_renames(self) -> None:
        """After rename, subsequent events use updated cache."""
        startrec = _get_last_eid() + 1
        test_dir = _unique_dir()
        try:
            a = os.path.join(test_dir, 'a.txt')
            b = os.path.join(test_dir, 'b.txt')
            with open(a, 'w') as f:
                f.write('start')
            os.rename(a, b)
            # Modify b — the source must resolve via updated cache.
            with open(b, 'a') as f:
                f.write(' more')

            time.sleep(0.5)
            src = LustreChangelogSource(
                mdt=MDT,
                mount_point=LUSTRE_MOUNT,
                fsname=FSNAME,
                poll_interval=0.0,
                startrec=startrec,
            )
            events = src.read()
            src.close()

            # MODIFIED events for b should have the correct path.
            modified = [
                e
                for e in events
                if e['kind'] == EventKind.MODIFIED and e['path'] == b
            ]
            assert len(modified) >= 1
        finally:
            shutil.rmtree(test_dir, ignore_errors=True)

    def test_cache_cleanup_on_delete(self) -> None:
        """After delete, cache no longer holds the FID."""
        startrec = _get_last_eid() + 1
        test_dir = _unique_dir()
        try:
            a = os.path.join(test_dir, 'del_cache.txt')
            with open(a, 'w') as f:
                f.write('will delete')
            os.remove(a)
            # Create a new file — should not conflict with deleted FID.
            b = os.path.join(test_dir, 'new_after_del.txt')
            with open(b, 'w') as f:
                f.write('new')

            time.sleep(0.5)
            events, changes = _source_and_pipeline(startrec)

            created = [e for e in events if e['kind'] == EventKind.CREATED]
            created_paths = [e['path'] for e in created]
            assert b in created_paths
        finally:
            shutil.rmtree(test_dir, ignore_errors=True)

    def test_cache_built_from_mkdir_chain(self) -> None:
        """FID cache correctly chains mkdir → creat across depths."""
        startrec = _get_last_eid() + 1
        test_dir = _unique_dir()
        try:
            # Create dirs one at a time to ensure FID cache chains correctly.
            d1 = os.path.join(test_dir, 'd1')
            d2 = os.path.join(d1, 'd2')
            d3 = os.path.join(d2, 'd3')
            os.mkdir(d1)
            os.mkdir(d2)
            os.mkdir(d3)
            leaf = os.path.join(d3, 'leaf.txt')
            with open(leaf, 'w') as f:
                f.write('deep leaf')

            time.sleep(0.5)
            src = LustreChangelogSource(
                mdt=MDT,
                mount_point=LUSTRE_MOUNT,
                fsname=FSNAME,
                poll_interval=0.0,
                startrec=startrec,
            )
            events = src.read()
            src.close()

            created = [e for e in events if e['kind'] == EventKind.CREATED]
            created_paths = [e['path'] for e in created]
            assert d1 in created_paths
            assert d2 in created_paths
            assert d3 in created_paths
            assert leaf in created_paths
        finally:
            shutil.rmtree(test_dir, ignore_errors=True)


# ===================================================================
# Iteration 37: BatchProcessor backward compat after GPFS merge
# ===================================================================
@skip_no_lustre
class TestBatchProcessorBackwardCompat:
    def test_default_slot_key_is_path(self) -> None:
        """Verify BatchProcessor defaults (slot_key='path', bypass_rename=True)
        still work correctly for Lustre events.
        """
        startrec = _get_last_eid() + 1
        test_dir = _unique_dir()
        try:
            filepath = os.path.join(test_dir, 'compat.txt')
            with open(filepath, 'w') as f:
                f.write('default params')
            os.rename(filepath, filepath + '.bak')
            time.sleep(0.5)

            src = LustreChangelogSource(
                mdt=MDT,
                mount_point=LUSTRE_MOUNT,
                fsname=FSNAME,
                poll_interval=0.0,
                startrec=startrec,
            )
            events = src.read()
            src.close()

            # Use default BatchProcessor (no explicit slot_key/bypass_rename).
            bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
            bp.add_events(events)
            coalesced = bp.flush()

            # RENAMED events should be at the end (bypass_rename=True default).
            renamed = [e for e in coalesced if e['kind'] == EventKind.RENAMED]
            assert len(renamed) == 2, f'Expected 2 RENAMED, got {len(renamed)}'

            # Non-RENAMED should come first.
            non_rename = [
                e for e in coalesced if e['kind'] != EventKind.RENAMED
            ]
            # Verify RENAMED are after all non-RENAMED in the list.
            if non_rename and renamed:
                last_non_rename_idx = max(
                    coalesced.index(e) for e in non_rename
                )
                first_rename_idx = min(coalesced.index(e) for e in renamed)
                assert first_rename_idx > last_non_rename_idx
        finally:
            shutil.rmtree(test_dir, ignore_errors=True)


# ===================================================================
# Iteration 38: Rename split across two reads (multi-batch pairing)
# ===================================================================
@skip_no_lustre
class TestRenameSplitAcrossReads:
    def test_create_in_read1_rename_in_read2(self) -> None:
        """Create file in read1, rename it in read2.

        The CREAT populates FID cache in read1. The RENME in read2
        must correctly resolve old_path via cached FID and emit the
        rename pair for StateManager.
        """
        test_dir = _unique_dir()
        try:
            sm = StateManager()
            startrec = _get_last_eid() + 1

            old = os.path.join(test_dir, 'before.txt')
            new = os.path.join(test_dir, 'after.txt')
            with open(old, 'w') as f:
                f.write('split rename test')
            time.sleep(0.5)

            src = LustreChangelogSource(
                mdt=MDT,
                mount_point=LUSTRE_MOUNT,
                fsname=FSNAME,
                poll_interval=0.0,
                startrec=startrec,
            )

            # Read 1: CREAT events.
            events1 = src.read()
            bp1 = BatchProcessor(rules=STAT_REDUCTION_RULES)
            bp1.add_events(events1)
            coalesced1 = bp1.flush()
            if coalesced1:
                sm.process_events(coalesced1)
            c1 = sm.emit()
            assert old in {c['path'] for c in c1 if c['op'] == 'update'}

            # Rename.
            os.rename(old, new)
            time.sleep(0.5)

            # Read 2: RENME events (different batch).
            events2 = src.read()
            src.close()

            renamed = [e for e in events2 if e['kind'] == EventKind.RENAMED]
            assert len(renamed) == 2, f'Expected 2 RENAMED, got {len(renamed)}'
            assert renamed[0]['path'] == old
            assert renamed[1]['path'] == new

            bp2 = BatchProcessor(rules=STAT_REDUCTION_RULES)
            bp2.add_events(events2)
            coalesced2 = bp2.flush()
            if coalesced2:
                sm.process_events(coalesced2)
            c2 = sm.emit()

            update2 = {c['path'] for c in c2 if c['op'] == 'update'}
            delete2 = {c['path'] for c in c2 if c['op'] == 'delete'}
            assert new in update2
            assert old in delete2
        finally:
            shutil.rmtree(test_dir, ignore_errors=True)


# ===================================================================
# Iteration 39: Swap two directories via temp
# ===================================================================
@skip_no_lustre
class TestSwapDirectories:
    def test_swap_dirs_via_temp(self) -> None:
        """Swap dir_a ↔ dir_b via temp: a→tmp, b→a, tmp→b.

        Each dir has children. After swap, a's children should be
        under b's name and vice versa.
        """
        startrec = _get_last_eid() + 1
        test_dir = _unique_dir()
        try:
            da = os.path.join(test_dir, 'dir_a')
            db = os.path.join(test_dir, 'dir_b')
            tmp = os.path.join(test_dir, 'dir_tmp')
            os.makedirs(da)
            os.makedirs(db)
            fa = os.path.join(da, 'a_file.txt')
            fb = os.path.join(db, 'b_file.txt')
            with open(fa, 'w') as f:
                f.write('from a')
            with open(fb, 'w') as f:
                f.write('from b')

            # Swap: a→tmp, b→a, tmp→b
            os.rename(da, tmp)
            os.rename(db, da)
            os.rename(tmp, db)

            time.sleep(0.5)
            _, changes = _source_and_pipeline(startrec)

            update_paths = {c['path'] for c in changes if c['op'] == 'update'}
            # After swap: dir_a should contain b_file.txt (was dir_b's)
            # and dir_b should contain a_file.txt (was dir_a's).
            assert os.path.join(da, 'b_file.txt') in update_paths
            assert os.path.join(db, 'a_file.txt') in update_paths
        finally:
            shutil.rmtree(test_dir, ignore_errors=True)


# ===================================================================
# Iteration 40: Mtime ordering across creates
# ===================================================================
@skip_no_lustre
class TestMtimeOrdering:
    def test_stat_mtime_increases_across_files(self) -> None:
        """Create files with 0.5s gaps — mtime should be monotonically
        non-decreasing across updates (stat at emit time).
        """
        startrec = _get_last_eid() + 1
        test_dir = _unique_dir()
        try:
            files = []
            for i in range(3):
                p = os.path.join(test_dir, f'seq_{i}.txt')
                with open(p, 'w') as f:
                    f.write(f'sequence {i}')
                files.append(p)
                if i < 2:
                    time.sleep(0.5)

            time.sleep(0.5)
            _, changes = _source_and_pipeline(startrec)

            mtimes = []
            for p in files:
                ups = [
                    c
                    for c in changes
                    if c['op'] == 'update' and c['path'] == p
                ]
                assert len(ups) >= 1, f'{p} missing from updates'
                mtimes.append(ups[-1]['stat']['mtime'])

            # mtime should be non-decreasing.
            for i in range(1, len(mtimes)):
                assert mtimes[i] >= mtimes[i - 1], (
                    f'mtime not monotonic: {mtimes}'
                )
        finally:
            shutil.rmtree(test_dir, ignore_errors=True)


# ===================================================================
# Iteration 41: Mixed directory and file renames in same batch
# ===================================================================
@skip_no_lustre
class TestMixedDirFileRenames:
    def test_dir_rename_and_file_rename_same_batch(self) -> None:
        """Rename a directory and an unrelated file in the same batch.

        Exercises rename pairing when directory RENME (with is_dir=True)
        and file RENME (is_dir=False) are interleaved.
        """
        startrec = _get_last_eid() + 1
        test_dir = _unique_dir()
        try:
            # Directory with child.
            subdir = os.path.join(test_dir, 'old_dir')
            os.makedirs(subdir)
            child = os.path.join(subdir, 'child.txt')
            with open(child, 'w') as f:
                f.write('child')
            # Standalone file.
            standalone = os.path.join(test_dir, 'old_file.txt')
            with open(standalone, 'w') as f:
                f.write('standalone')

            # Rename both.
            new_dir = os.path.join(test_dir, 'new_dir')
            new_file = os.path.join(test_dir, 'new_file.txt')
            os.rename(subdir, new_dir)
            os.rename(standalone, new_file)

            time.sleep(0.5)
            _, changes = _source_and_pipeline(startrec)

            update_paths = {c['path'] for c in changes if c['op'] == 'update'}
            assert new_dir in update_paths
            assert os.path.join(new_dir, 'child.txt') in update_paths
            assert new_file in update_paths
        finally:
            shutil.rmtree(test_dir, ignore_errors=True)


# ===================================================================
# Iteration 42: Create many files then rename parent directory
# ===================================================================
@skip_no_lustre
class TestManyFilesRenameDir:
    def test_create_20_files_rename_parent(self) -> None:
        """Create dir with 20 files, rename dir — all 20 at new path."""
        startrec = _get_last_eid() + 1
        test_dir = _unique_dir()
        try:
            subdir = os.path.join(test_dir, 'old')
            os.makedirs(subdir)
            names = []
            for i in range(20):
                name = f'f_{i:02d}.txt'
                with open(os.path.join(subdir, name), 'w') as f:
                    f.write(f'file {i}')
                names.append(name)

            new_dir = os.path.join(test_dir, 'new')
            os.rename(subdir, new_dir)

            time.sleep(1.0)
            _, changes = _source_and_pipeline(startrec)

            update_paths = {c['path'] for c in changes if c['op'] == 'update'}
            missing = [
                n
                for n in names
                if os.path.join(new_dir, n) not in update_paths
            ]
            assert len(missing) == 0, (
                f'{len(missing)}/20 missing: {missing[:5]}'
            )
        finally:
            shutil.rmtree(test_dir, ignore_errors=True)


# ===================================================================
# Iteration 43: Stat accuracy for directory (is_dir flag)
# ===================================================================
@skip_no_lustre
class TestDirStatAccuracy:
    def test_dir_stat_has_correct_mode(self) -> None:
        """Created directory should have stat with directory mode bit set."""
        startrec = _get_last_eid() + 1
        test_dir = _unique_dir()
        try:
            subdir = os.path.join(test_dir, 'statdir')
            os.makedirs(subdir, mode=0o755)

            time.sleep(0.5)
            _, changes = _source_and_pipeline(startrec)

            dir_updates = [
                c
                for c in changes
                if c['op'] == 'update' and c['path'] == subdir
            ]
            assert len(dir_updates) >= 1
            mode = dir_updates[-1]['stat']['mode']
            # Should be a directory (S_ISDIR).
            assert stat.S_ISDIR(mode), f'Not a directory mode: {oct(mode)}'
        finally:
            shutil.rmtree(test_dir, ignore_errors=True)


# ===================================================================
# Iteration 44: Incremental rename: dir rename then new child then
#   rename child (all in separate reads)
# ===================================================================
@skip_no_lustre
class TestIncrementalDirOps:
    def test_rename_dir_create_child_rename_child_3_reads(self) -> None:
        """Read1: create dir+file. Read2: rename dir. Read3: create new
        file inside + rename the old file inside.

        3 reads exercising FID cache evolution through dir rename then
        child operations.
        """
        test_dir = _unique_dir()
        try:
            sm = StateManager()
            startrec = _get_last_eid() + 1

            subdir = os.path.join(test_dir, 'v1')
            os.makedirs(subdir)
            f1 = os.path.join(subdir, 'f1.txt')
            with open(f1, 'w') as f:
                f.write('f1')
            time.sleep(0.5)

            src = LustreChangelogSource(
                mdt=MDT,
                mount_point=LUSTRE_MOUNT,
                fsname=FSNAME,
                poll_interval=0.0,
                startrec=startrec,
            )

            def _read():
                ev = src.read()
                bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
                bp.add_events(ev)
                co = bp.flush()
                if co:
                    sm.process_events(co)
                return sm.emit()

            # Read 1: create.
            _read()

            # Read 2: rename dir.
            new_dir = os.path.join(test_dir, 'v2')
            os.rename(subdir, new_dir)
            time.sleep(0.5)
            _read()

            # Read 3: create new file + rename existing.
            f2 = os.path.join(new_dir, 'f2.txt')
            with open(f2, 'w') as f:
                f.write('f2')
            f1_new = os.path.join(new_dir, 'f1_renamed.txt')
            os.rename(os.path.join(new_dir, 'f1.txt'), f1_new)
            time.sleep(0.5)
            c3 = _read()
            src.close()

            update3 = {c['path'] for c in c3 if c['op'] == 'update'}
            assert f2 in update3, f'New file missing: {update3}'
            assert f1_new in update3, f'Renamed child missing: {update3}'
        finally:
            shutil.rmtree(test_dir, ignore_errors=True)


# ===================================================================
# Iteration 45: Truncate pre-existing file (fid2path fallback + TRUNC)
# ===================================================================
@skip_no_lustre
class TestTruncatePreexisting:
    def test_truncate_preexisting_via_pipeline(self) -> None:
        """Pre-existing file truncated after startrec.

        Source resolves via fid2path. Since the file was never CREATED
        in StateManager, the MODIFIED event from TRUNC goes through
        _handle_updated which skips untracked paths. This is expected
        behavior — verify no crash and no phantom updates.
        """
        test_dir = _unique_dir()
        try:
            filepath = os.path.join(test_dir, 'preexist_trunc.txt')
            with open(filepath, 'w') as f:
                f.write('has content')
            time.sleep(0.3)

            startrec = _get_last_eid() + 1
            # Truncate via open('w').
            with open(filepath, 'w') as f:
                pass
            time.sleep(0.5)

            events, changes = _source_and_pipeline(startrec)

            # Should have MODIFIED events from CLOSE/TRUNC.
            modified = [e for e in events if e['kind'] == EventKind.MODIFIED]
            assert len(modified) >= 1

            # But no update in pipeline (file never tracked).
            update_paths = {c['path'] for c in changes if c['op'] == 'update'}
            assert filepath not in update_paths
        finally:
            shutil.rmtree(test_dir, ignore_errors=True)


# ===================================================================
# Iteration 46: Event count verification (received vs reduced vs accepted)
# ===================================================================
@skip_no_lustre
class TestBatchStatsLive:
    def test_batch_stats_accurate_for_create_modify_delete(self) -> None:
        """Create file, modify 5x, delete → verify batch stats reflect
        reduction (MODIFIED absorbed, then REMOVED cancels CREATED).

        received = CREAT + 5*CLOSE + UNLNK + overhead (parent CLOSE etc)
        reduced > 0 (MODIFIED absorbed by CREATED, then cancel)
        accepted = small number after reduction
        """
        startrec = _get_last_eid() + 1
        test_dir = _unique_dir()
        try:
            filepath = os.path.join(test_dir, 'stats_test.txt')
            with open(filepath, 'w') as f:
                f.write('v0')
            for i in range(5):
                with open(filepath, 'a') as f:
                    f.write(f' v{i + 1}')
            os.remove(filepath)

            time.sleep(0.5)
            src = LustreChangelogSource(
                mdt=MDT,
                mount_point=LUSTRE_MOUNT,
                fsname=FSNAME,
                poll_interval=0.0,
                startrec=startrec,
            )
            events = src.read()
            src.close()

            bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
            bp.add_events(events)

            # Stats should show reduction happened.
            assert bp.received > 0
            assert bp.reduced > 0, 'Expected some events to be reduced'
            assert bp.accepted < bp.received, (
                f'accepted ({bp.accepted}) should be < received ({bp.received})'
            )
            # After commit + pipeline, no update for the file (cancelled).
            coalesced = bp.flush()
            sm = StateManager()
            if coalesced:
                sm.process_events(coalesced)
            changes = sm.emit()
            assert filepath not in {c['path'] for c in changes}
        finally:
            shutil.rmtree(test_dir, ignore_errors=True)
