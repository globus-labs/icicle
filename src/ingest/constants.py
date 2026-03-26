"""Shared constants for the ingest pipeline."""

from __future__ import annotations

NUM_WORKERS: int = 64

# Worker divisors by directory depth level.
# Deeper prefixes use fewer workers to avoid sparsity.
WORKER_DIVISORS: dict[int, int] = {
    2: 2,  # depth 2 -> 32 effective workers
    3: 4,  # depth 3 -> 16 effective workers
}
DEFAULT_DEEP_DIVISOR: int = 8  # depth >= 4 -> 8 effective workers

# Quantile breakpoints used by all sketch stats functions.
QUANTILE_BREAKPOINTS: tuple[tuple[str, float], ...] = (
    ('10p', 0.1),
    ('25p', 0.25),
    ('median', 0.5),
    ('75p', 0.75),
    ('90p', 0.9),
    ('99p', 0.99),
)


def derive_worker_id(base_id: int, depth: int) -> int:
    """Derive a coarser worker ID for deeper directory prefixes."""
    divisor = WORKER_DIVISORS.get(depth, DEFAULT_DEEP_DIVISOR)
    if depth < min(WORKER_DIVISORS):
        return base_id
    return base_id // divisor
