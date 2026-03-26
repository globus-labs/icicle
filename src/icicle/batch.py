"""Event coalescer with reduction rules for icicle."""

from __future__ import annotations

from enum import StrEnum
from enum import unique
from typing import Any

from src.icicle.events import EventKind


@unique
class ReductionAction(StrEnum):
    """Possible outcomes of applying a reduction rule."""

    ACCEPT = 'accept'
    IGNORE = 'ignore'  # drop new event (prior absorbs it)
    CANCEL = 'cancel'  # drop both new and matching prior


class BatchProcessor:
    """Accumulates events by slot key and applies reduction rules."""

    def __init__(
        self,
        rules: (
            dict[EventKind, tuple[ReductionAction, set[EventKind]]] | None
        ) = None,
        slot_key: str = 'path',
        bypass_rename: bool = True,
    ) -> None:
        """Initialize with optional custom reduction rules.

        Args:
            rules: Reduction rules mapping an incoming event kind to
                an action and the set of prior kinds that trigger it.
                Defaults to no rules (all events pass through).
            slot_key: Event dict key used for slotting (default 'path',
                      use 'inode' for GPFS).
            bypass_rename: If True, RENAMED events bypass slotting
                          and are kept in order for StateManager
                          pairing (fswatch/Lustre). If False,
                          RENAMED events go through normal slotting
                          (GPFS — no pairing needed).
        """
        self.slots: dict[str, list[dict[str, Any]]] = {}
        self.rules: dict[
            EventKind,
            tuple[ReductionAction, set[EventKind]],
        ] = rules if rules is not None else {}
        self._slot_key = slot_key
        self._bypass_rename = bypass_rename
        self.received = 0
        self.ignored = 0
        self.cancelled = 0
        self.accepted = 0
        # RENAMED events are kept in order (not slotted by path) because
        # StateManager pairs consecutive RENAMED events. Slotting by path
        # would separate old-path and new-path events of the same rename.
        self._rename_events: list[dict[str, Any]] = []

    @property
    def reduced(self) -> int:
        """Total reduced events (ignored + cancelled)."""
        return self.ignored + self.cancelled

    def add_events(self, events: list[dict[str, Any]]) -> None:
        """Add events, applying reduction rules per slot."""
        for event in events:
            self.received += 1

            if self._bypass_rename and event['kind'] == EventKind.RENAMED:
                self._rename_events.append(event)
                self.accepted += 1
                continue

            key = str(event[self._slot_key])
            slot = self.slots.setdefault(key, [])

            result = self._apply_reduction(event, slot)
            if result == ReductionAction.IGNORE:
                self.ignored += 1
                continue
            if result == ReductionAction.CANCEL:
                self.cancelled += 2
                self.accepted -= 1
                continue

            slot.append(event)
            self.accepted += 1

    def flush(self) -> list[dict[str, Any]]:
        """Flush all slots and return coalesced events."""
        batch: list[dict[str, Any]] = []
        for slot in self.slots.values():
            batch.extend(slot)
        self.slots.clear()
        # Append rename events at the end, preserving their order.
        batch.extend(self._rename_events)
        self._rename_events.clear()
        return batch

    def _apply_reduction(
        self,
        event: dict[str, Any],
        slot: list[dict[str, Any]],
    ) -> ReductionAction:
        """Return the reduction outcome for this event."""
        rule = self.rules.get(event['kind'])
        if not rule:
            return ReductionAction.ACCEPT

        action, target_kinds = rule
        for idx in range(len(slot) - 1, -1, -1):
            prev = slot[idx]
            if prev['kind'] in target_kinds:
                if action == ReductionAction.CANCEL:
                    slot.pop(idx)
                return action

        return ReductionAction.ACCEPT
