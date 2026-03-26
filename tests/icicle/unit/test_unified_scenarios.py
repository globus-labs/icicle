"""Parameterized cross-backend scenario tests.

Each (backend, scenario) combination builds events in the right format,
runs them through ``BatchProcessor -> StateManager -> emit``, and asserts
the expected output.  This validates that every backend produces the same
logical outcome for the same logical operation.
"""

from __future__ import annotations

import pytest

from src.icicle.batch import BatchProcessor
from tests.icicle.backend_config import ALL_BACKENDS
from tests.icicle.scenarios import SCENARIOS


@pytest.mark.parametrize('backend', ALL_BACKENDS, ids=lambda b: b.name)
@pytest.mark.parametrize('scenario', SCENARIOS, ids=lambda s: s.name)
def test_scenario(backend, scenario):
    batches = scenario.make_events(backend)
    bp = BatchProcessor(
        rules=backend.reduction_rules,
        slot_key=backend.slot_key,
        bypass_rename=backend.bypass_rename,
    )
    state = backend.state_factory()

    collected: list[dict] = []
    for batch in batches:
        bp.add_events(batch)
        coalesced = bp.flush()
        if coalesced:
            state.process_events(coalesced)
            collected.extend(state.emit())
    # Final flush — pick up any remaining pending state
    collected.extend(state.emit())

    scenario.expected(collected)
