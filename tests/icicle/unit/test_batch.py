"""Extensive tests for icicle BatchProcessor reduction rules."""

from __future__ import annotations

from src.icicle.batch import BatchProcessor
from src.icicle.events import EventKind
from src.icicle.fswatch_events import STAT_REDUCTION_RULES


def ev(path: str, kind: EventKind, **kw) -> dict:
    """Helper to create minimal event dicts."""
    return {'path': path, 'kind': kind, 'ts': 0.0, 'is_dir': False, **kw}


# ---------------------------------------------------------------------------
# Basic pass-through
# ---------------------------------------------------------------------------


class TestPassThrough:
    def test_single_created(self):
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events([ev('/a', EventKind.CREATED)])
        batch = bp.flush()
        assert len(batch) == 1
        assert batch[0]['kind'] == EventKind.CREATED

    def test_single_modified(self):
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events([ev('/a', EventKind.MODIFIED)])
        batch = bp.flush()
        assert len(batch) == 1

    def test_single_removed(self):
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events([ev('/a', EventKind.REMOVED)])
        batch = bp.flush()
        assert len(batch) == 1

    def test_single_renamed(self):
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events([ev('/a', EventKind.RENAMED)])
        batch = bp.flush()
        assert len(batch) == 1

    def test_single_accessed(self):
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events([ev('/a', EventKind.MODIFIED)])
        batch = bp.flush()
        assert len(batch) == 1


# ---------------------------------------------------------------------------
# Ignore rules (DROP_NEW)
# ---------------------------------------------------------------------------


class TestIgnoreRules:
    def test_modified_after_modified(self):
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events(
            [
                ev('/a', EventKind.MODIFIED),
                ev('/a', EventKind.MODIFIED),
            ],
        )
        assert len(bp.flush()) == 1

    def test_modified_after_created(self):
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events(
            [
                ev('/a', EventKind.CREATED),
                ev('/a', EventKind.MODIFIED),
            ],
        )
        batch = bp.flush()
        assert len(batch) == 1
        assert batch[0]['kind'] == EventKind.CREATED

    def test_accessed_after_accessed(self):
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events(
            [
                ev('/a', EventKind.MODIFIED),
                ev('/a', EventKind.MODIFIED),
            ],
        )
        assert len(bp.flush()) == 1

    def test_accessed_after_created(self):
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events(
            [
                ev('/a', EventKind.CREATED),
                ev('/a', EventKind.MODIFIED),
            ],
        )
        batch = bp.flush()
        assert len(batch) == 1
        assert batch[0]['kind'] == EventKind.CREATED

    def test_accessed_after_modified(self):
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events(
            [
                ev('/a', EventKind.MODIFIED),
                ev('/a', EventKind.MODIFIED),
            ],
        )
        batch = bp.flush()
        assert len(batch) == 1
        assert batch[0]['kind'] == EventKind.MODIFIED

    def test_100_modified_same_path(self):
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events([ev('/a', EventKind.MODIFIED) for _ in range(100)])
        assert len(bp.flush()) == 1

    def test_created_absorbs_all(self):
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events(
            [
                ev('/a', EventKind.CREATED),
                ev('/a', EventKind.MODIFIED),
                ev('/a', EventKind.MODIFIED),
                ev('/a', EventKind.MODIFIED),
            ],
        )
        batch = bp.flush()
        assert len(batch) == 1
        assert batch[0]['kind'] == EventKind.CREATED


# ---------------------------------------------------------------------------
# Cancel rules (CANCEL_BOTH)
# ---------------------------------------------------------------------------


class TestCancelRules:
    def test_created_then_removed(self):
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events(
            [
                ev('/a', EventKind.CREATED),
                ev('/a', EventKind.REMOVED),
            ],
        )
        assert len(bp.flush()) == 0

    def test_created_modified_removed(self):
        """MODIFIED absorbed by CREATED, then REMOVED cancels CREATED."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events(
            [
                ev('/a', EventKind.CREATED),
                ev('/a', EventKind.MODIFIED),
                ev('/a', EventKind.REMOVED),
            ],
        )
        assert len(bp.flush()) == 0

    def test_created_modified_modified_removed(self):
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events(
            [
                ev('/a', EventKind.CREATED),
                ev('/a', EventKind.MODIFIED),
                ev('/a', EventKind.MODIFIED),
                ev('/a', EventKind.REMOVED),
            ],
        )
        assert len(bp.flush()) == 0

    def test_modified_then_removed_not_cancelled(self):
        """REMOVED does NOT cancel MODIFIED (Robinhood alignment)."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events(
            [
                ev('/a', EventKind.MODIFIED),
                ev('/a', EventKind.REMOVED),
            ],
        )
        assert len(bp.flush()) == 2


# ---------------------------------------------------------------------------
# Complex sequences
# ---------------------------------------------------------------------------


class TestComplexSequences:
    def test_created_removed_created(self):
        """First pair cancels, second CREATED survives."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events(
            [
                ev('/a', EventKind.CREATED),
                ev('/a', EventKind.REMOVED),
                ev('/a', EventKind.CREATED),
            ],
        )
        batch = bp.flush()
        assert len(batch) == 1
        assert batch[0]['kind'] == EventKind.CREATED

    def test_removed_no_prior_created(self):
        """REMOVED with no prior CREATED passes through (nothing to cancel)."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events([ev('/a', EventKind.REMOVED)])
        batch = bp.flush()
        assert len(batch) == 1
        assert batch[0]['kind'] == EventKind.REMOVED

    def test_renamed_never_reduced(self):
        """RENAMED events always pass through."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events(
            [
                ev('/a', EventKind.RENAMED),
                ev('/a', EventKind.RENAMED),
                ev('/a', EventKind.RENAMED),
            ],
        )
        assert len(bp.flush()) == 3

    def test_multiple_create_delete_cycles(self):
        """CREAT→REMOVE repeated 3 times → all cancelled."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        events = []
        for _ in range(3):
            events.append(ev('/a', EventKind.CREATED))
            events.append(ev('/a', EventKind.REMOVED))
        bp.add_events(events)
        assert len(bp.flush()) == 0

    def test_create_delete_cycles_with_final_create(self):
        """3 create-delete cycles then a final create → 1 CREATED."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        events = []
        for _ in range(3):
            events.append(ev('/a', EventKind.CREATED))
            events.append(ev('/a', EventKind.REMOVED))
        events.append(ev('/a', EventKind.CREATED))
        bp.add_events(events)
        batch = bp.flush()
        assert len(batch) == 1
        assert batch[0]['kind'] == EventKind.CREATED

    def test_interleaved_events_different_paths(self):
        """Events for different paths don't interfere."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events(
            [
                ev('/a', EventKind.CREATED),
                ev('/b', EventKind.CREATED),
                ev('/a', EventKind.MODIFIED),
                ev('/b', EventKind.REMOVED),
            ],
        )
        batch = bp.flush()
        # /a: CREATED (MODIFIED absorbed) = 1 event
        # /b: CREATED + REMOVED = cancelled
        assert len(batch) == 1
        assert batch[0]['path'] == '/a'

    def test_removed_does_not_cancel_renamed(self):
        """REMOVED cancels modify events but not RENAMED."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events(
            [
                ev('/a', EventKind.RENAMED),
                ev('/a', EventKind.REMOVED),
            ],
        )
        # RENAMED is not a modify event, so REMOVED can't cancel it
        batch = bp.flush()
        assert len(batch) == 2


# ---------------------------------------------------------------------------
# Multi-path independence
# ---------------------------------------------------------------------------


class TestMultiPath:
    def test_data_modified_independent_paths(self):
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events(
            [
                ev('/a', EventKind.MODIFIED),
                ev('/b', EventKind.MODIFIED),
            ],
        )
        assert len(bp.flush()) == 2

    def test_created_removed_different_paths(self):
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events(
            [
                ev('/a', EventKind.CREATED),
                ev('/b', EventKind.REMOVED),
            ],
        )
        assert len(bp.flush()) == 2

    def test_many_paths_all_independent(self):
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events(
            [ev(f'/file_{i}', EventKind.CREATED) for i in range(100)],
        )
        assert len(bp.flush()) == 100


# ---------------------------------------------------------------------------
# Stats counters
# ---------------------------------------------------------------------------


class TestStatsCounters:
    def test_basic_counts(self):
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events(
            [
                ev('/a', EventKind.CREATED),
                ev('/a', EventKind.MODIFIED),  # ignored
                ev('/a', EventKind.REMOVED),  # cancels CREATED
            ],
        )
        assert bp.received == 3
        # 1 ignored (MODIFIED) + 2 cancelled (REMOVED + CREATED) = 3
        assert bp.reduced == 3
        assert bp.accepted == 0

    def test_counts_after_commit(self):
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events([ev('/a', EventKind.CREATED)])
        bp.flush()
        # Counters persist across commits (cumulative)
        assert bp.received == 1
        assert bp.accepted == 1
        assert bp.reduced == 0

    def test_all_ignored_counts(self):
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events([ev('/a', EventKind.MODIFIED) for _ in range(10)])
        assert bp.received == 10
        assert bp.accepted == 1
        assert bp.reduced == 9

    def test_empty_batch(self):
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        assert bp.flush() == []
        assert bp.received == 0


# ---------------------------------------------------------------------------
# Complex reduction sequences
# ---------------------------------------------------------------------------


class TestComplexReductionSequences:
    def test_created_50_modified_removed_created_modified(self):
        """CREATED + 50×MODIFIED + REMOVED + CREATED + MODIFIED → 1 CREATED."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        events = [ev('/a', EventKind.CREATED)]
        events.extend(ev('/a', EventKind.MODIFIED) for _ in range(50))
        events.append(ev('/a', EventKind.REMOVED))
        events.append(ev('/a', EventKind.CREATED))
        events.append(ev('/a', EventKind.MODIFIED))
        bp.add_events(events)
        batch = bp.flush()
        assert len(batch) == 1
        assert batch[0]['kind'] == EventKind.CREATED

    def test_modified_removed_created_modified_removed(self):
        """MODIFIED + REMOVED + CREATED + MODIFIED + REMOVED.

        REMOVED only cancels CREATED (Robinhood). The initial
        MODIFIED + REMOVED survive, then CREATED + MODIFIED are
        reduced to CREATED, which is then cancelled by the final
        REMOVED.  Result: 2 events (initial MODIFIED + REMOVED).
        """
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events(
            [
                ev('/a', EventKind.MODIFIED),
                ev('/a', EventKind.REMOVED),
                ev('/a', EventKind.CREATED),
                ev('/a', EventKind.MODIFIED),
                ev('/a', EventKind.REMOVED),
            ],
        )
        assert len(bp.flush()) == 2

    def test_created_modified_modified_modified_removed(self):
        """CREATED + 3×MODIFIED + REMOVED → empty (ephemeral)."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events(
            [
                ev('/a', EventKind.CREATED),
                ev('/a', EventKind.MODIFIED),
                ev('/a', EventKind.MODIFIED),
                ev('/a', EventKind.MODIFIED),
                ev('/a', EventKind.REMOVED),
            ],
        )
        assert len(bp.flush()) == 0

    def test_5_create_delete_cycles_final_modified(self):
        """5× (CREATED + REMOVED) + CREATED + MODIFIED → 1 CREATED."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        events = []
        for _ in range(5):
            events.append(ev('/a', EventKind.CREATED))
            events.append(ev('/a', EventKind.REMOVED))
        events.append(ev('/a', EventKind.CREATED))
        events.append(ev('/a', EventKind.MODIFIED))
        bp.add_events(events)
        batch = bp.flush()
        assert len(batch) == 1
        assert batch[0]['kind'] == EventKind.CREATED

    def test_stats_for_full_cancellation(self):
        """CREATED + 10×MODIFIED + REMOVED → ephemeral file.

        10 MODIFIED are absorbed by CREATED (IGNORE), then REMOVED
        cancels CREATED (CANCEL).  12 received, 12 reduced, 0 accepted.
        """
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        events = [ev('/a', EventKind.CREATED)]
        events.extend(ev('/a', EventKind.MODIFIED) for _ in range(10))
        events.append(ev('/a', EventKind.REMOVED))
        bp.add_events(events)
        assert bp.received == 12
        assert bp.reduced == 12
        assert bp.accepted == 0


# ---------------------------------------------------------------------------
# ACCESSED interleaving
# ---------------------------------------------------------------------------


class TestAccessedInterleaving:
    def test_accessed_absorbed_by_all_modify_family(self):
        """ACCESSED is absorbed by CREATED, MODIFIED, or prior ACCESSED."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events(
            [ev('/a', EventKind.CREATED), ev('/a', EventKind.MODIFIED)],
        )
        bp.add_events(
            [ev('/b', EventKind.MODIFIED), ev('/b', EventKind.MODIFIED)],
        )
        bp.add_events(
            [ev('/c', EventKind.MODIFIED), ev('/c', EventKind.MODIFIED)],
        )
        batch = bp.flush()
        assert len(batch) == 3
        kinds = {e['path']: e['kind'] for e in batch}
        assert kinds['/a'] == EventKind.CREATED
        assert kinds['/b'] == EventKind.MODIFIED
        assert kinds['/c'] == EventKind.MODIFIED

    def test_accessed_then_created_both_pass(self):
        """ACCESSED then CREATED — CREATED has no rule, both pass."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events(
            [ev('/a', EventKind.MODIFIED), ev('/a', EventKind.CREATED)],
        )
        assert len(bp.flush()) == 2

    def test_modified_removed_not_cancelled(self):
        """MODIFIED + REMOVED → both survive (Robinhood alignment)."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events(
            [ev('/a', EventKind.MODIFIED), ev('/a', EventKind.REMOVED)],
        )
        assert len(bp.flush()) == 2

    def test_accessed_across_paths_independent(self):
        """ACCESSED for different paths don't interfere."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events(
            [
                ev('/a', EventKind.MODIFIED),
                ev('/b', EventKind.MODIFIED),
                ev('/a', EventKind.MODIFIED),
                ev('/b', EventKind.CREATED),
            ],
        )
        batch = bp.flush()
        a_events = [e for e in batch if e['path'] == '/a']
        b_events = [e for e in batch if e['path'] == '/b']
        assert len(a_events) == 1  # MODIFIED ignored
        assert len(b_events) == 2  # CREATED passes through


# ---------------------------------------------------------------------------
# Attribute change + rename in batch
# ---------------------------------------------------------------------------


class TestAttrRenameBatch:
    def test_modified_and_rename_same_path(self):
        """MODIFIED + RENAMED pair — slots and renames don't interfere."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events(
            [
                ev('/a', EventKind.MODIFIED),
                ev('/a', EventKind.RENAMED),
                ev('/b', EventKind.RENAMED),
            ],
        )
        batch = bp.flush()
        assert len(batch) == 3
        assert batch[0]['kind'] == EventKind.MODIFIED
        assert batch[1]['kind'] == EventKind.RENAMED
        assert batch[2]['kind'] == EventKind.RENAMED

    def test_created_modified_renamed_in_one_batch(self):
        """CREATED + 3×MODIFIED + RENAMED pair → CREATED + 2 RENAMED."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events(
            [
                ev('/new.txt', EventKind.CREATED),
                ev('/new.txt', EventKind.MODIFIED),
                ev('/new.txt', EventKind.MODIFIED),
                ev('/new.txt', EventKind.MODIFIED),
                ev('/new.txt', EventKind.RENAMED),
                ev('/final.txt', EventKind.RENAMED),
            ],
        )
        batch = bp.flush()
        assert len(batch) == 3
        assert batch[0]['kind'] == EventKind.CREATED


# ---------------------------------------------------------------------------
# Bulk create + remove cancellation
# ---------------------------------------------------------------------------


class TestBulkCreateRemove:
    def test_100_created_100_removed(self):
        """100 CREATED + 100 REMOVED → all cancelled."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        events = [ev(f'/file_{i}', EventKind.CREATED) for i in range(100)]
        events.extend(ev(f'/file_{i}', EventKind.REMOVED) for i in range(100))
        bp.add_events(events)
        assert len(bp.flush()) == 0
        assert bp.received == 200
        assert bp.accepted == 0

    def test_100_created_50_removed(self):
        """100 CREATED, 50 REMOVED → 50 survive."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        events = [ev(f'/file_{i}', EventKind.CREATED) for i in range(100)]
        events.extend(ev(f'/file_{i}', EventKind.REMOVED) for i in range(50))
        bp.add_events(events)
        batch = bp.flush()
        assert len(batch) == 50

    def test_interleaved_create_remove_100(self):
        """100× (CREATED + REMOVED) interleaved → all cancelled."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        events = []
        for i in range(100):
            events.append(ev(f'/f_{i}', EventKind.CREATED))
            events.append(ev(f'/f_{i}', EventKind.REMOVED))
        bp.add_events(events)
        assert len(bp.flush()) == 0


# ---------------------------------------------------------------------------
# Commit behavior
# ---------------------------------------------------------------------------


class TestBatchCommitBehavior:
    def test_double_commit_second_empty(self):
        """commit() twice — second returns empty."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events([ev('/a', EventKind.CREATED)])
        assert len(bp.flush()) == 1
        assert len(bp.flush()) == 0

    def test_add_after_commit_independent(self):
        """Add after commit — new batch independent."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events([ev('/a', EventKind.CREATED)])
        bp.flush()
        bp.add_events([ev('/b', EventKind.MODIFIED)])
        batch = bp.flush()
        assert len(batch) == 1
        assert batch[0]['path'] == '/b'

    def test_rename_events_cleared_on_commit(self):
        """RENAMED events cleared after commit."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events(
            [ev('/a', EventKind.RENAMED), ev('/b', EventKind.RENAMED)],
        )
        assert len(bp.flush()) == 2
        assert len(bp.flush()) == 0

    def test_two_creates_same_path_no_rule(self):
        """Two CREATEDs same path — no reduction rule, both pass."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events(
            [ev('/a', EventKind.CREATED), ev('/a', EventKind.CREATED)],
        )
        assert len(bp.flush()) == 2


# ---------------------------------------------------------------------------
# Mixed batch ordering
# ---------------------------------------------------------------------------


class TestMixedBatchOrdering:
    def test_slots_before_renames_in_commit(self):
        """Commit returns slot events before rename events."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events(
            [
                ev('/new.txt', EventKind.CREATED),
                ev('/old.txt', EventKind.RENAMED),
                ev('/new_name.txt', EventKind.RENAMED),
                ev('/existing.txt', EventKind.MODIFIED),
                ev('/dir_old', EventKind.RENAMED, is_dir=True),
                ev('/dir_new', EventKind.RENAMED, is_dir=True),
            ],
        )
        batch = bp.flush()

        slot_events = [e for e in batch if e['kind'] != EventKind.RENAMED]
        rename_events = [e for e in batch if e['kind'] == EventKind.RENAMED]
        assert len(slot_events) == 2
        assert len(rename_events) == 4
        assert max(batch.index(e) for e in slot_events) < min(
            batch.index(e) for e in rename_events
        )


# ---------------------------------------------------------------------------
# Multiple add_events accumulation
# ---------------------------------------------------------------------------


class TestMultiAddEvents:
    def test_5_calls_before_commit(self):
        """5 add_events calls then commit — all accumulated."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        for i in range(5):
            bp.add_events([ev(f'/file_{i}.txt', EventKind.CREATED)])
        assert len(bp.flush()) == 5

    def test_reduction_across_calls(self):
        """Reduction works across multiple add_events for same path."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events([ev('/a', EventKind.CREATED)])
        bp.add_events([ev('/a', EventKind.MODIFIED)])
        bp.add_events([ev('/a', EventKind.REMOVED)])
        assert len(bp.flush()) == 0

    def test_rename_order_preserved_across_calls(self):
        """RENAMED from different add_events maintain order."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events([ev('/a', EventKind.RENAMED)])
        bp.add_events([ev('/b', EventKind.RENAMED)])
        batch = bp.flush()
        assert batch[0]['path'] == '/a'
        assert batch[1]['path'] == '/b'

    def test_mixed_paths_across_calls(self):
        """Different paths across calls — independent slots."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events([ev('/a', EventKind.CREATED)])
        bp.add_events([ev('/b', EventKind.CREATED)])
        bp.add_events([ev('/a', EventKind.REMOVED)])
        batch = bp.flush()
        assert len(batch) == 1
        assert batch[0]['path'] == '/b'


# ---------------------------------------------------------------------------
# Standalone REMOVED behavior
# ---------------------------------------------------------------------------


class TestStandaloneRemoved:
    def test_removed_no_prior(self):
        """REMOVED with no prior event passes through."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events([ev('/pre', EventKind.REMOVED)])
        assert len(bp.flush()) == 1

    def test_removed_different_path(self):
        """REMOVED for different path — independent slots."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events(
            [ev('/a', EventKind.CREATED), ev('/b', EventKind.REMOVED)],
        )
        assert len(bp.flush()) == 2

    def test_double_removed_same_path(self):
        """Two REMOVEDs same path — both pass (REMOVED not in _ANY_MODIFY)."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events(
            [ev('/a', EventKind.REMOVED), ev('/a', EventKind.REMOVED)],
        )
        assert len(bp.flush()) == 2


# ---------------------------------------------------------------------------
# RENAMED-only batch
# ---------------------------------------------------------------------------


class TestRenamedOnlyBatch:
    def test_all_renamed_pass_through(self):
        """All RENAMED events pass in order, no slots created."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events(
            [
                ev('/a', EventKind.RENAMED),
                ev('/b', EventKind.RENAMED),
                ev('/c', EventKind.RENAMED),
                ev('/d', EventKind.RENAMED),
            ],
        )
        assert len(bp.slots) == 0
        batch = bp.flush()
        assert [e['path'] for e in batch] == ['/a', '/b', '/c', '/d']

    def test_renamed_stats(self):
        """RENAMED: all accepted, none reduced."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events(
            [ev('/a', EventKind.RENAMED), ev('/b', EventKind.RENAMED)],
        )
        bp.flush()
        assert bp.received == 2
        assert bp.accepted == 2
        assert bp.reduced == 0


# ---------------------------------------------------------------------------
# Custom reduction rules
# ---------------------------------------------------------------------------


class TestCustomRules:
    def test_custom_ignore_rule(self):
        """Custom rule: CREATED ignored if MODIFIED in slot."""
        bp = BatchProcessor(
            rules={EventKind.CREATED: ('ignore', {EventKind.MODIFIED})},
        )
        bp.add_events(
            [ev('/a', EventKind.MODIFIED), ev('/a', EventKind.CREATED)],
        )
        assert len(bp.flush()) == 1

    def test_empty_rules_no_reduction(self):
        """Empty rules — nothing reduced."""
        bp = BatchProcessor(rules={})
        bp.add_events(
            [
                ev('/a', EventKind.CREATED),
                ev('/a', EventKind.MODIFIED),
                ev('/a', EventKind.REMOVED),
            ],
        )
        assert len(bp.flush()) == 3

    def test_none_rules_passes_all(self):
        """rules=None defaults to empty (no reduction)."""
        bp = BatchProcessor(rules=None)
        bp.add_events(
            [ev('/a', EventKind.CREATED), ev('/a', EventKind.MODIFIED)],
        )
        assert len(bp.flush()) == 2


# ---------------------------------------------------------------------------
# Slot cleanup verification
# ---------------------------------------------------------------------------


class TestSlotCleanupInternal:
    def test_slots_empty_after_commit(self):
        """Slots dict empty after commit."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events(
            [ev('/a', EventKind.CREATED), ev('/b', EventKind.MODIFIED)],
        )
        assert len(bp.slots) == 2
        bp.flush()
        assert len(bp.slots) == 0

    def test_rename_list_empty_after_commit(self):
        """_rename_events empty after commit."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events([ev('/a', EventKind.RENAMED)])
        bp.flush()
        assert len(bp._rename_events) == 0

    def test_independent_slots_across_commits(self):
        """Each commit starts fresh — no cross-batch reduction."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events([ev('/a', EventKind.CREATED)])
        bp.flush()
        bp.add_events([ev('/a', EventKind.MODIFIED)])
        batch = bp.flush()
        assert len(batch) == 1  # MODIFIED passes (no CREATED in this batch)


# ---------------------------------------------------------------------------
# Same path in slot and rename events
# ---------------------------------------------------------------------------


class TestSamePathSlotAndRename:
    def test_modified_and_renamed_independent(self):
        """MODIFIED in slot, RENAMED in _rename_events — independent."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events(
            [
                ev('/a', EventKind.MODIFIED),
                ev('/a', EventKind.RENAMED),
                ev('/b', EventKind.RENAMED),
            ],
        )
        batch = bp.flush()
        assert len(batch) == 3

    def test_created_removed_cancelled_renamed_survives(self):
        """CREATED+REMOVED cancelled, RENAMED survives."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events(
            [
                ev('/a', EventKind.CREATED),
                ev('/a', EventKind.REMOVED),
                ev('/a', EventKind.RENAMED),
                ev('/b', EventKind.RENAMED),
            ],
        )
        batch = bp.flush()
        assert len(batch) == 2
        assert all(e['kind'] == EventKind.RENAMED for e in batch)


# ---------------------------------------------------------------------------
# RENAMED + REMOVED interaction in batch
# ---------------------------------------------------------------------------


class TestRenamedRemovedBatch:
    def test_renamed_and_removed_same_path(self):
        """RENAMED goes to _rename_events, REMOVED to slot — both pass."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events(
            [
                ev('/a', EventKind.RENAMED),
                ev('/b', EventKind.RENAMED),
                ev('/a', EventKind.REMOVED),
            ],
        )
        batch = bp.flush()
        assert len(batch) == 3

    def test_created_renamed_removed_same_path(self):
        """CREATED+REMOVED cancelled, RENAMED survives."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events(
            [
                ev('/a', EventKind.CREATED),
                ev('/a', EventKind.RENAMED),
                ev('/b', EventKind.RENAMED),
                ev('/a', EventKind.REMOVED),
            ],
        )
        batch = bp.flush()
        assert len(batch) == 2
        assert all(e['kind'] == EventKind.RENAMED for e in batch)


# ---------------------------------------------------------------------------
# Batch processor reuse across cycles
# ---------------------------------------------------------------------------


class TestBatchReuse:
    def test_10_cycles_counters_accumulate(self):
        """10 commit cycles — counters cumulative."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        for i in range(10):
            bp.add_events([ev(f'/f_{i}', EventKind.CREATED)])
            bp.flush()
        assert bp.received == 10
        assert bp.accepted == 10

    def test_each_cycle_independent(self):
        """Prior CREATED doesn't affect next cycle's MODIFIED."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events([ev('/a', EventKind.CREATED)])
        bp.flush()
        bp.add_events([ev('/a', EventKind.MODIFIED)])
        assert len(bp.flush()) == 1

    def test_commit_returns_fresh_list(self):
        """Clearing returned list doesn't affect next commit."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events([ev('/a', EventKind.CREATED)])
        bp.flush().clear()
        bp.add_events([ev('/b', EventKind.CREATED)])
        assert len(bp.flush()) == 1


# ---------------------------------------------------------------------------
# All event kinds in one batch
# ---------------------------------------------------------------------------


class TestAllKindsBatch:
    def test_all_5_kinds_different_paths(self):
        """One of each kind, different paths — all 5 pass."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events(
            [
                ev('/a', EventKind.CREATED),
                ev('/b', EventKind.MODIFIED),
                ev('/c', EventKind.MODIFIED),
                ev('/d', EventKind.REMOVED),
                ev('/e', EventKind.RENAMED),
            ],
        )
        assert len(bp.flush()) == 5

    def test_all_5_kinds_same_path(self):
        """All kinds for same path — complex reduction."""
        bp = BatchProcessor(rules=STAT_REDUCTION_RULES)
        bp.add_events(
            [
                ev('/x', EventKind.CREATED),
                ev('/x', EventKind.MODIFIED),
                ev('/x', EventKind.MODIFIED),
                ev('/x', EventKind.REMOVED),
                ev('/x', EventKind.RENAMED),
            ],
        )
        batch = bp.flush()
        assert len(batch) == 1
        assert batch[0]['kind'] == EventKind.RENAMED
