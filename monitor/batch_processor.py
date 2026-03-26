"""Abstract batch processor interfaces for icicle2."""

from __future__ import annotations

import json
import logging
import time
from abc import ABC
from abc import abstractmethod
from collections.abc import Hashable
from dataclasses import asdict
from typing import Any

from monitor.conf import BatchProcessorOptions
from monitor.data_types import EventType
from monitor.data_types import INOTIFY_REDUCTION_RULES
from monitor.data_types import REDUCTION_RULES
from monitor.data_types import ReductionResult

logger = logging.getLogger(__name__)


class BatchProcessor(ABC):
    """Generic template for batch processors handling changelog events."""

    def __init__(self) -> None:
        """Initialize counters."""
        self.slots: dict[Hashable, list[dict[str, Any]]] = {}  # ordered
        self.pending_events = 0

        self._first_update_time: float | None = None  # TODO: not used
        self._last_update_time: float | None = None
        self._ev_received = 0
        self._ev_reduced = 0
        self._ev_accepted = 0

    def add_events(self, events: list[dict[str, Any]]) -> None:
        """Record events so they can be committed in a later batch."""
        for event in events:
            self._ev_received += 1
            slot_key = self._determine_slot_key(event)
            slot = self.slots.setdefault(slot_key, [])

            reduction = self._apply_reduction_rule(event, slot)
            if reduction is ReductionResult.DROP_NEW:
                self._ev_reduced += 1
                continue
            if reduction is ReductionResult.DROP_NEW_AND_PREV:
                self.pending_events -= 1
                self._ev_reduced += 2
                self._ev_accepted -= 1
                continue

            self._add_to_slot(event, slot)  # _ev_accepted inside

    def commit_batch(self) -> list[dict[str, Any]]:
        """Flush accumulated events and reset internal state."""
        batch: list[dict[str, Any]] = []
        for slot in self.slots.values():
            batch.extend(slot)

        self.slots.clear()
        self.pending_events = 0
        self._first_update_time = None
        self._last_update_time = None

        return batch

    @abstractmethod
    def _determine_slot_key(self, event: dict[str, Any]) -> Hashable:
        """Return the key used to group events into slots."""

    @abstractmethod
    def _apply_reduction_rule(
        self,
        event: dict[str, Any],
        slot: list[dict[str, Any]],
    ) -> ReductionResult:
        """Apply per-event reduction logic; override where appropriate."""

    def _add_to_slot(
        self,
        event: dict[str, Any],
        slot: list[dict[str, Any]],
    ) -> None:
        """Append the event, track counters, and update timers."""
        slot.append(event)
        self._ev_accepted += 1
        self.pending_events += 1

        now = time.time()
        if self._first_update_time is None:
            self._first_update_time = now
        self._last_update_time = now

    def stats_summary(self) -> dict[str, int]:
        """Return batch counters as a dictionary."""
        return {
            'bp_received': self._ev_received,
            'bp_reduced': self._ev_reduced,
            'bp_accepted': self._ev_accepted,
        }


class GPFSBatchProcessor(BatchProcessor):
    """Batch processor tuned for GPFS changelog events."""

    def __init__(
        self,
        cfg: BatchProcessorOptions,
    ) -> None:
        """Configure GPFS-specific batching parameters."""
        super().__init__()
        self.cfg = cfg
        self.rules = INOTIFY_REDUCTION_RULES

    def _determine_slot_key(self, event: dict[str, Any]) -> Hashable:
        return event['inode']

    def _apply_reduction_rule(
        self,
        event: dict[str, Any],
        slot: list[dict[str, Any]],
    ) -> ReductionResult:
        if not self.cfg.enable_reduction_rules:
            return ReductionResult.ACCEPT

        rule = self.rules.get(event['event_type'])
        if not rule:
            return ReductionResult.ACCEPT

        action, target_types = rule
        for index in range(len(slot) - 1, -1, -1):
            prev = slot[index]
            if action == 'cancel' and prev['event_type'] in target_types:
                slot.pop(index)
                return ReductionResult.DROP_NEW_AND_PREV

        return ReductionResult.ACCEPT


class LFSBatchProcessor(BatchProcessor):
    """Batch processor tuned for GPFS changelog events."""

    def __init__(
        self,
        cfg: BatchProcessorOptions,
    ) -> None:
        """Configure GPFS-specific batching parameters."""
        super().__init__()
        self.cfg = cfg
        self.rules = REDUCTION_RULES

    def _determine_slot_key(self, event: dict[str, Any]) -> Hashable:
        slot_key = event['target_fid']
        if event['event_type'] == EventType.RENME:
            assert event['source_fid']  # make mypy happy
            slot_key = event['source_fid']
        return slot_key

    def _apply_reduction_rule(
        self,
        event: dict[str, Any],
        slot: list[dict[str, Any]],
    ) -> ReductionResult:
        """Apply reduction rules for an event."""
        if not self.cfg.enable_reduction_rules:
            return ReductionResult.ACCEPT

        rule = self.rules.get(event['event_type'])
        if not rule:
            return ReductionResult.ACCEPT

        action, target_types = rule
        for index in range(len(slot) - 1, -1, -1):
            prev_event = slot[index]
            if (
                prev_event['event_type'] == EventType.RENME
                and action == 'cancel'
            ):
                # If the current event is UNLNK or RMDIR, a 'cancel' rule
                # may apply. However, if a RENME event is found first, the
                # sequence of events cannot be reduced.
                logger.debug(
                    'Reduction Rule Skipped: A %s event is being processed, but a %s event was found first in slot. The event sequence is too complex to reduce.',  # noqa: E501
                    event['event_type'],
                    prev_event['event_type'],
                    # slot['fid'],
                )
                return ReductionResult.ACCEPT

            if prev_event['event_type'] in target_types:
                logger.debug(
                    'Reduction Rule Applied: New %s event encountered prior %s in slot.',  # noqa: E501
                    event['event_type'],
                    prev_event['event_type'],
                    # slot['fid'],
                )

                if action == 'ignore':
                    logger.debug(
                        '--> Outcome: Ignoring new %s event.',
                        event['event_type'],
                    )
                    return ReductionResult.DROP_NEW  # Ignore the new event.

                if action == 'cancel':
                    logger.debug(
                        '--> Outcome: Cancelling new %s and prior %s events.',
                        event['event_type'],
                        prev_event['event_type'],
                    )
                    slot.pop(index)
                    return (
                        ReductionResult.DROP_NEW_AND_PREV
                    )  # Cancel both new and prior events.

        return ReductionResult.ACCEPT  # No matching prior event found.


__all__ = [
    'BatchProcessor',
    'GPFSBatchProcessor',
    'ReductionResult',
]

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    )
    options = BatchProcessorOptions()
    logger.info(json.dumps(asdict(options), indent=2, default=str))

    # batch_processor = GPFSBatchProcessor(options)
    # logger.info(batch_processor.stats_summary())

    batch_processor = LFSBatchProcessor(options)
    logger.info(batch_processor.stats_summary())
