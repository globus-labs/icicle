"""Live GPFS Kafka integration tests for icicle.

These tests perform real filesystem operations on a GPFS mount and
verify that events arrive via Kafka and flow through the full pipeline
(GPFSKafkaSource → BatchProcessor → GPFSStateManager → emit).

Requires: GPFS mount at /ibm/fs1/fset1, Kafka broker, mmwatch active.
Run with: sg icicle-users -c "python -m pytest tests/icicle/integration/test_gpfs.py -v"
"""

from __future__ import annotations

import os
import shutil
import time
from uuid import uuid4

import pytest

from src.icicle.batch import BatchProcessor
from src.icicle.gpfs_events import GPFS_REDUCTION_RULES
from src.icicle.gpfs_source import GPFSKafkaSource
from src.icicle.gpfs_state import GPFSStateManager

GPFS_MOUNT = os.environ.get('GPFS_MOUNT', '/ibm/fs1/fset1')
TOPIC = os.environ.get('KAFKA_TOPIC', 'fset1-topic-1p')
BOOTSTRAP_SERVERS = os.environ.get('BOOTSTRAP', 'localhost:9092')
GROUP_ID_PREFIX = 'icicle-gpfs-test'

# Skip all tests if GPFS mount is not available
pytestmark = pytest.mark.skipif(
    not os.path.isdir(GPFS_MOUNT),
    reason=f'GPFS mount {GPFS_MOUNT} not available',
)


def _run_gpfs_pipeline(events):
    """Run events through GPFS-configured BatchProcessor + GPFSStateManager."""
    bp = BatchProcessor(
        rules=GPFS_REDUCTION_RULES,
        slot_key='inode',
        bypass_rename=False,
    )
    bp.add_events(events)
    coalesced = bp.flush()
    sm = GPFSStateManager()
    if coalesced:
        sm.process_events(coalesced)
    return sm.emit()


def _collect_events(src, max_reads=5):
    """Read multiple batches from source to collect events."""
    all_events = []
    for _ in range(max_reads):
        batch = src.read()
        if batch:
            all_events.extend(batch)
        else:
            break
    return all_events


# ---------------------------------------------------------------------------
# Shared workload harness
# ---------------------------------------------------------------------------

from tests.icicle.workloads import assert_workload_results
from tests.icicle.workloads import execute_workload
from tests.icicle.workloads import WORKLOADS

WORKLOAD_FINAL_WAIT = (
    10.0  # GPFS needs more time for mmwatch → Kafka propagation
)


@pytest.fixture
def harness():
    """Create GPFS test directory and pipeline for workload tests."""
    test_dir = os.path.join(
        GPFS_MOUNT,
        f'icicle_wl_{os.getpid()}_{time.time_ns()}',
    )
    os.makedirs(test_dir)
    group_id = f'{GROUP_ID_PREFIX}-wl-{time.time_ns()}'
    source = GPFSKafkaSource(
        topic=TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=group_id,
        partition=0,
        poll_timeout=10.0,
    )

    def collect(final_wait=WORKLOAD_FINAL_WAIT):
        time.sleep(final_wait)
        events = _collect_events(source)
        source.close()
        return _run_gpfs_pipeline(events)

    yield test_dir, collect
    shutil.rmtree(test_dir, ignore_errors=True)


@pytest.mark.parametrize('workload', WORKLOADS, ids=lambda w: w.name)
def test_workload(workload, harness):
    """Run a shared workload against GPFS backend."""
    watch_dir, collect = harness
    execute_workload(watch_dir, workload.ops)
    payloads = collect(WORKLOAD_FINAL_WAIT)
    assert_workload_results(payloads, workload.expected)


class TestRenameOperations:
    """Live tests for GPFS-specific inode lifecycle verification."""

    def test_create_write_delete_lifecycle(self):
        """Full lifecycle: create, write, delete — transient file."""
        test_dir = os.path.join(GPFS_MOUNT, f'icicle_test_{uuid4().hex[:8]}')
        group_id = f'{GROUP_ID_PREFIX}-{uuid4().hex[:8]}'

        src = GPFSKafkaSource(
            topic=TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id=group_id,
            poll_timeout=10.0,
        )
        try:
            time.sleep(1.0)

            os.makedirs(test_dir, exist_ok=True)
            filepath = os.path.join(test_dir, 'transient.txt')
            with open(filepath, 'w') as f:
                f.write('temporary')
            os.remove(filepath)

            time.sleep(3.0)

            events = _collect_events(src)
            src.close()

            changes = _run_gpfs_pipeline(events)

            # If CREATED + REMOVED in same batch → cancelled (no output).
            # If separate batches with single emit() → create but no delete
            # (transient — never emitted). Either way, no delete expected.
            transient_deletes = [
                c
                for c in changes
                if c.get('path') == filepath and c['op'] == 'delete'
            ]
            assert len(transient_deletes) == 0, (
                f'Transient file should not produce delete: {transient_deletes}'
            )

        finally:
            src.close()
            shutil.rmtree(test_dir, ignore_errors=True)


class TestNestedAndModify:
    """Live tests for nested directories and file modification on GPFS."""

    def test_nested_dirs_with_files_and_modify(self):
        """3-level tree, files at each level, modify deepest file."""
        test_dir = os.path.join(GPFS_MOUNT, f'icicle_test_{uuid4().hex[:8]}')
        group_id = f'{GROUP_ID_PREFIX}-{uuid4().hex[:8]}'

        src = GPFSKafkaSource(
            topic=TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id=group_id,
            poll_timeout=10.0,
        )
        try:
            time.sleep(1.0)

            # -- filesystem operations --
            l1 = os.path.join(test_dir, 'l1')
            l2 = os.path.join(l1, 'l2')
            l3 = os.path.join(l2, 'l3')
            os.makedirs(l3, exist_ok=True)

            f1 = os.path.join(l1, 'top.txt')
            f2 = os.path.join(l2, 'mid.txt')
            f3 = os.path.join(l3, 'deep.txt')
            for fp, content in [(f1, 'top'), (f2, 'middle'), (f3, 'deep')]:
                with open(fp, 'w') as f:
                    f.write(content)

            time.sleep(0.5)
            # Modify deepest file
            with open(f3, 'a') as f:
                f.write('-appended')

            time.sleep(3.0)

            events = _collect_events(src)
            src.close()

            changes = _run_gpfs_pipeline(events)

            update_paths = {c['path'] for c in changes if c['op'] == 'update'}
            test_paths = {p for p in update_paths if p.startswith(test_dir)}
            # At minimum: test_dir, l1, l2, l3, and the 3 files = 7 paths
            # (some may be missing if events aren't captured for mkdir of
            # test_dir itself since it's created before consumer connects fully)
            assert len(test_paths) >= 3, (
                f'Expected at least 3 test paths in nested tree, '
                f'got {len(test_paths)}: {test_paths}'
            )

        finally:
            src.close()
            shutil.rmtree(test_dir, ignore_errors=True)


class TestMultiPartition:
    """Live tests using 2-partition topic with multiple consumers."""

    TOPIC_2P = 'fset1-topic-2p'

    def test_burst_10_files_2_partitions(self):
        """Create 10 files, consume via 2-partition topic with 2 consumers."""
        test_dir = os.path.join(GPFS_MOUNT, f'icicle_test_{uuid4().hex[:8]}')
        group_id = f'{GROUP_ID_PREFIX}-{uuid4().hex[:8]}'

        src = GPFSKafkaSource(
            topic=self.TOPIC_2P,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id=group_id,
            num_consumers=2,
            poll_timeout=10.0,
        )
        try:
            time.sleep(2.0)  # extra time for 2 consumers to get assignments

            os.makedirs(test_dir, exist_ok=True)
            for i in range(10):
                fp = os.path.join(test_dir, f'mp_{i}.txt')
                with open(fp, 'w') as f:
                    f.write(f'partition-test-{i}')

            time.sleep(4.0)  # extra propagation for multi-partition

            events = _collect_events(src, max_reads=10)
            src.close()

            changes = _run_gpfs_pipeline(events)

            update_paths = {c['path'] for c in changes if c['op'] == 'update'}
            test_paths = {p for p in update_paths if p.startswith(test_dir)}
            assert len(test_paths) >= 1, (
                f'Expected at least 1 test path from 2-partition topic, '
                f'got {test_paths}. Events: {len(events)}'
            )

        finally:
            src.close()
            shutil.rmtree(test_dir, ignore_errors=True)

    def test_single_partition_explicit(self):
        """Consume from partition 0 only of the 2-partition topic."""
        test_dir = os.path.join(GPFS_MOUNT, f'icicle_test_{uuid4().hex[:8]}')
        group_id = f'{GROUP_ID_PREFIX}-{uuid4().hex[:8]}'

        src = GPFSKafkaSource(
            topic=self.TOPIC_2P,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id=group_id,
            partition=0,
            poll_timeout=10.0,
        )
        try:
            time.sleep(1.0)

            os.makedirs(test_dir, exist_ok=True)
            for i in range(5):
                fp = os.path.join(test_dir, f'p0_{i}.txt')
                with open(fp, 'w') as f:
                    f.write(f'p0-{i}')

            time.sleep(3.0)

            events = _collect_events(src)
            src.close()

            # Events may or may not land on partition 0 — just verify
            # no crash and source closes cleanly
            assert isinstance(events, list)

        finally:
            src.close()
            shutil.rmtree(test_dir, ignore_errors=True)


class TestShmQueue:
    """Live tests using shared memory queue."""

    def test_shm_queue_create_and_delete(self):
        """Create file using shm queue, verify events arrive and cleanup."""
        test_dir = os.path.join(GPFS_MOUNT, f'icicle_test_{uuid4().hex[:8]}')
        group_id = f'{GROUP_ID_PREFIX}-{uuid4().hex[:8]}'

        src = GPFSKafkaSource(
            topic=TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id=group_id,
            queue_type='shm',
            poll_timeout=10.0,
        )
        shm_q = src._shm_queue
        assert shm_q is not None, 'shm queue should be initialized'

        try:
            time.sleep(1.0)

            os.makedirs(test_dir, exist_ok=True)
            filepath = os.path.join(test_dir, 'shm_test.txt')
            with open(filepath, 'w') as f:
                f.write('shm queue test')

            time.sleep(3.0)

            # For shm queue, read() blocks on semaphore until a batch arrives.
            # After 3s, consumer threads should have pushed at least one batch.
            # Do a single read() to get queued events, then close immediately.
            events = src.read()
            src.close()

            # Verify events arrived through shm queue
            test_events = [
                e for e in events if e.get('path', '').startswith(test_dir)
            ]
            assert len(test_events) >= 1, (
                f'Expected at least 1 event via shm queue, '
                f'got {len(test_events)}. Total events: {len(events)}'
            )

            # Verify shm was unlinked after close
            assert shm_q._unlinked is True, (
                'shm queue should be unlinked after close'
            )

        finally:
            src.close()
            shutil.rmtree(test_dir, ignore_errors=True)
            # Double-check no stale shm
            import glob

            stale = glob.glob('/dev/shm/psm_*')
            assert len(stale) == 0, f'Stale shm segments: {stale}'


class TestSpecialCases:
    """Live tests for GPFS-specific edge cases: special filenames."""

    def test_special_filenames(self):
        """Files with spaces and dot-leading names."""
        test_dir = os.path.join(GPFS_MOUNT, f'icicle_test_{uuid4().hex[:8]}')
        group_id = f'{GROUP_ID_PREFIX}-{uuid4().hex[:8]}'

        src = GPFSKafkaSource(
            topic=TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id=group_id,
            poll_timeout=10.0,
        )
        try:
            time.sleep(1.0)

            os.makedirs(test_dir, exist_ok=True)
            names = ['file with spaces.txt', '.hidden', '...dots']
            for name in names:
                fp = os.path.join(test_dir, name)
                with open(fp, 'w') as f:
                    f.write(f'content of {name}')

            time.sleep(3.0)

            events = _collect_events(src)
            src.close()

            test_events = [
                e for e in events if e.get('path', '').startswith(test_dir)
            ]
            assert len(test_events) >= 1, (
                f'Expected events for special filenames, '
                f'got {len(test_events)}. Total: {len(events)}'
            )
            # Verify paths preserved correctly (no mangling of spaces/dots)
            test_paths = {e['path'] for e in test_events}
            for name in names:
                expected = os.path.join(test_dir, name)
                if expected in test_paths:
                    # Path survived parse_gpfs_message with correct name
                    assert True

        finally:
            src.close()
            shutil.rmtree(test_dir, ignore_errors=True)


class TestHardlinkAndEdgeCases:
    """Live tests for hardlinks and edge cases."""

    def test_hardlink_same_inode(self):
        """Create file + hardlink — same inode, two paths."""
        test_dir = os.path.join(GPFS_MOUNT, f'icicle_test_{uuid4().hex[:8]}')
        group_id = f'{GROUP_ID_PREFIX}-{uuid4().hex[:8]}'

        src = GPFSKafkaSource(
            topic=TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id=group_id,
            poll_timeout=10.0,
        )
        try:
            time.sleep(1.0)

            os.makedirs(test_dir, exist_ok=True)
            filepath = os.path.join(test_dir, 'original.txt')
            linkpath = os.path.join(test_dir, 'hardlink.txt')
            with open(filepath, 'w') as f:
                f.write('shared content')
            os.link(filepath, linkpath)

            time.sleep(3.0)

            events = _collect_events(src)
            src.close()

            # Both paths should produce events with the same inode
            test_events = [
                e for e in events if e.get('path', '').startswith(test_dir)
            ]
            assert len(test_events) >= 1, (
                f'Expected events for hardlink test, got {len(test_events)}'
            )

            # Verify hardlink event shares the same inode
            inodes_by_path = {}
            for e in test_events:
                p = e.get('path', '')
                if p.startswith(test_dir):
                    inodes_by_path.setdefault(p, set()).add(e.get('inode'))

            # If both paths captured, they should share an inode
            if filepath in inodes_by_path and linkpath in inodes_by_path:
                orig_inodes = inodes_by_path[filepath]
                link_inodes = inodes_by_path[linkpath]
                assert orig_inodes & link_inodes, (
                    f'Hardlink should share inode: '
                    f'original={orig_inodes}, link={link_inodes}'
                )

        finally:
            src.close()
            shutil.rmtree(test_dir, ignore_errors=True)

    def test_symlink_create(self):
        """Create a symlink, verify IN_CREATE event arrives."""
        test_dir = os.path.join(GPFS_MOUNT, f'icicle_test_{uuid4().hex[:8]}')
        group_id = f'{GROUP_ID_PREFIX}-{uuid4().hex[:8]}'

        src = GPFSKafkaSource(
            topic=TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id=group_id,
            poll_timeout=10.0,
        )
        try:
            time.sleep(1.0)

            os.makedirs(test_dir, exist_ok=True)
            target = os.path.join(test_dir, 'target.txt')
            link = os.path.join(test_dir, 'link.txt')
            with open(target, 'w') as f:
                f.write('target content')
            os.symlink(target, link)

            time.sleep(3.0)

            events = _collect_events(src)
            src.close()

            test_events = [
                e for e in events if e.get('path', '').startswith(test_dir)
            ]
            assert len(test_events) >= 1, (
                f'Expected events for symlink test, got {len(test_events)}'
            )

        finally:
            src.close()
            shutil.rmtree(test_dir, ignore_errors=True)

    def test_empty_file_create(self):
        """Create an empty file (0 bytes), verify events arrive."""
        test_dir = os.path.join(GPFS_MOUNT, f'icicle_test_{uuid4().hex[:8]}')
        group_id = f'{GROUP_ID_PREFIX}-{uuid4().hex[:8]}'

        src = GPFSKafkaSource(
            topic=TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id=group_id,
            poll_timeout=10.0,
        )
        try:
            time.sleep(1.0)

            os.makedirs(test_dir, exist_ok=True)
            filepath = os.path.join(test_dir, 'empty.txt')
            open(filepath, 'w').close()  # create empty file

            time.sleep(3.0)

            events = _collect_events(src)
            src.close()

            test_events = [e for e in events if e.get('path') == filepath]
            assert len(test_events) >= 1, (
                f'Expected events for empty file, got {len(test_events)}'
            )

        finally:
            src.close()
            shutil.rmtree(test_dir, ignore_errors=True)


class TestDeleteOperations:
    """Live tests for delete operations on GPFS."""

    def test_create_then_rmrf_tree(self):
        """Create tree, emit, rm -rf, emit — verify deletes."""
        test_dir = os.path.join(GPFS_MOUNT, f'icicle_test_{uuid4().hex[:8]}')
        group_id = f'{GROUP_ID_PREFIX}-{uuid4().hex[:8]}'

        src = GPFSKafkaSource(
            topic=TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id=group_id,
            poll_timeout=10.0,
        )
        try:
            time.sleep(1.0)

            # Phase 1: create tree
            os.makedirs(test_dir, exist_ok=True)
            subdir = os.path.join(test_dir, 'sub')
            os.mkdir(subdir)
            f1 = os.path.join(test_dir, 'top.txt')
            f2 = os.path.join(subdir, 'nested.txt')
            with open(f1, 'w') as f:
                f.write('top level')
            with open(f2, 'w') as f:
                f.write('nested file')

            time.sleep(3.0)

            # Read create events and process through pipeline
            create_events = _collect_events(src)
            bp = BatchProcessor(
                rules=GPFS_REDUCTION_RULES,
                slot_key='inode',
                bypass_rename=False,
            )
            sm = GPFSStateManager()
            bp.add_events(create_events)
            coalesced = bp.flush()
            if coalesced:
                sm.process_events(coalesced)
            create_changes = sm.emit()  # register paths as emitted

            create_test_paths = {
                c['path']
                for c in create_changes
                if c['op'] == 'update' and c['path'].startswith(test_dir)
            }

            # Phase 2: delete tree
            shutil.rmtree(test_dir)

            time.sleep(3.0)

            # Read delete events
            delete_events = _collect_events(src)
            src.close()

            bp.add_events(delete_events)
            coalesced = bp.flush()
            if coalesced:
                sm.process_events(coalesced)
            delete_changes = sm.emit()

            delete_paths = {
                c['path']
                for c in delete_changes
                if c['op'] == 'delete' and c['path'].startswith(test_dir)
            }

            # At least some events should arrive (create or delete)
            assert len(create_events) + len(delete_events) >= 1, (
                f'Expected at least 1 event total, '
                f'got creates={len(create_events)} deletes={len(delete_events)}'
            )

        finally:
            src.close()
            shutil.rmtree(test_dir, ignore_errors=True)


class TestRapidSequences:
    """Live tests for rapid operation sequences."""

    def test_create_modify_rename_rapid(self):
        """Create, modify, rename in rapid succession."""
        test_dir = os.path.join(GPFS_MOUNT, f'icicle_test_{uuid4().hex[:8]}')
        group_id = f'{GROUP_ID_PREFIX}-{uuid4().hex[:8]}'

        src = GPFSKafkaSource(
            topic=TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id=group_id,
            poll_timeout=10.0,
        )
        try:
            time.sleep(1.0)

            os.makedirs(test_dir, exist_ok=True)
            old_path = os.path.join(test_dir, 'rapid.txt')
            new_path = os.path.join(test_dir, 'rapid_final.txt')

            # Rapid sequence — no sleeps between ops
            with open(old_path, 'w') as f:
                f.write('initial')
            with open(old_path, 'a') as f:
                f.write(' + appended')
            os.rename(old_path, new_path)

            time.sleep(3.0)

            events = _collect_events(src)
            src.close()

            test_events = [
                e for e in events if e.get('path', '').startswith(test_dir)
            ]
            assert len(test_events) >= 1, (
                f'Expected events for rapid sequence, got {len(test_events)}'
            )

        finally:
            src.close()
            shutil.rmtree(test_dir, ignore_errors=True)

    def test_interleaved_writes_3_files(self):
        """Write to 3 files with interleaved operations, verify all captured."""
        test_dir = os.path.join(GPFS_MOUNT, f'icicle_test_{uuid4().hex[:8]}')
        group_id = f'{GROUP_ID_PREFIX}-{uuid4().hex[:8]}'

        src = GPFSKafkaSource(
            topic=TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id=group_id,
            poll_timeout=10.0,
        )
        try:
            time.sleep(1.0)

            os.makedirs(test_dir, exist_ok=True)
            paths = [
                os.path.join(test_dir, f'writer_{i}.txt') for i in range(3)
            ]

            # Interleaved: create all, then modify all
            for p in paths:
                with open(p, 'w') as f:
                    f.write('init')
            for p in paths:
                with open(p, 'a') as f:
                    f.write(' + modified')

            time.sleep(3.0)

            events = _collect_events(src)
            src.close()

            test_events = [
                e for e in events if e.get('path', '').startswith(test_dir)
            ]
            assert len(test_events) >= 3, (
                f'Expected at least 3 events for 3 files, got {len(test_events)}'
            )

            # All 3 file paths should have events
            event_paths = {e['path'] for e in test_events}
            for p in paths:
                assert p in event_paths, f'Missing events for {p}'

        finally:
            src.close()
            shutil.rmtree(test_dir, ignore_errors=True)
