from __future__ import annotations

import random
import string
import zlib
from collections import Counter

from constants import NUM_WORKERS


def crc32_worker_id(value: str, num_workers: int = NUM_WORKERS) -> int:
    worker_id = zlib.crc32(value.encode('utf-8')) & 0xFFFFFFFF
    return worker_id % num_workers


def _random_string(rng: random.Random, n: int = 32) -> str:
    letters = string.ascii_letters + string.digits
    return ''.join(rng.choices(letters, k=n))


def test_crc32_worker_id_within_range():
    rng = random.Random(0)
    for _ in range(100):
        worker_id = crc32_worker_id(_random_string(rng))
        assert 0 <= worker_id < NUM_WORKERS


def test_crc32_worker_id_distribution_reasonable():
    rng = random.Random(42)
    counts = Counter()
    num_samples = 20_000

    for _ in range(num_samples):
        counts[crc32_worker_id(_random_string(rng))] += 1

    avg = num_samples / NUM_WORKERS
    # Allow for some variance but ensure distribution is not too skewed.
    assert max(counts.values()) <= avg * 1.35
    assert min(counts.values()) >= avg * 0.65
