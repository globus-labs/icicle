from __future__ import annotations

import base64
import json
import math
from collections.abc import Iterable
from collections.abc import Sequence

from datasketches import kll_floats_sketch
from datasketches import req_floats_sketch
from datasketches import tdigest_float
from ddsketch import DDSketch
from ddsketch.pb import ddsketch_pb2 as pb
from ddsketch.pb.proto import DDSketchProto


def _expected_quantiles(data: Sequence[float], quantiles: Iterable[float]):
    sorted_data = sorted(data)
    last_index = len(sorted_data) - 1
    return [sorted_data[int(q * last_index)] for q in quantiles]


def _build_kll_sketch(values: Iterable[float], k: int = 200):
    sketch = kll_floats_sketch(k=k)
    for value in values:
        sketch.update(value)
    return sketch


def _build_req_floats_sketch(
    values: Iterable[float],
    k: int = 12,
    is_hra: bool = True,
):
    sketch = req_floats_sketch(k=k, is_hra=is_hra)
    for value in values:
        sketch.update(float(value))
    return sketch


def _build_dd_sketch(values: Iterable[float]):
    sketch = DDSketch()
    for value in values:
        sketch.add(value)
    return sketch


def _build_tdigest(values: Iterable[float], k: int = 200):
    digest = tdigest_float(k=k)
    for value in values:
        digest.update(float(value))
    return digest


def test_kllsketch_quantiles_merge_and_serialization():
    quantiles = [0.25, 0.5, 0.75, 0.99]
    base_data = list(range(5_000))
    secondary_data = list(range(10_000, 12_500))

    sketch = _build_kll_sketch(base_data)
    min_value = sketch.get_min_value()
    max_value = sketch.get_max_value()
    assert min_value == 0, (
        f'Initial KLL min mismatch: expected 0, got {min_value}'
    )
    assert max_value == 4_999, (
        f'Initial KLL max mismatch: expected 4_999, got {max_value}'
    )

    expected_quantiles = _expected_quantiles(base_data, quantiles)
    actual_quantiles = sketch.get_quantiles(quantiles)
    abs_tolerance = (
        len(base_data) * 0.02
    )  # generous error bound for approximate rank

    for quantile, expected, actual in zip(
        quantiles,
        expected_quantiles,
        actual_quantiles,
    ):
        assert math.isclose(actual, expected, abs_tol=abs_tolerance), (
            f'KLL quantile {quantile}: expected around {expected} +/- {abs_tolerance}, got {actual}'
        )

    merged_sketch = _build_kll_sketch(secondary_data)
    sketch.merge(merged_sketch)

    merged_min = sketch.get_min_value()
    merged_max = sketch.get_max_value()
    assert merged_min == 0, (
        f'Merged KLL min mismatch: expected 0, got {merged_min}'
    )
    assert merged_max == 12_499, (
        f'Merged KLL max mismatch: expected 12_499, got {merged_max}'
    )

    combined_data = base_data + secondary_data
    combined_expected = _expected_quantiles(combined_data, quantiles)
    combined_actual = sketch.get_quantiles(quantiles)

    for quantile, expected, actual in zip(
        quantiles,
        combined_expected,
        combined_actual,
    ):
        assert math.isclose(actual, expected, abs_tol=abs_tolerance), (
            f'KLL post-merge quantile {quantile}: expected around {expected} +/- {abs_tolerance}, got {actual}'
        )

    serialized = sketch.serialize()
    encoded = base64.b64encode(serialized).decode('ascii')
    json_payload = json.dumps({'kllsketch': encoded})

    decoded_payload = json.loads(json_payload)
    restored_bytes = base64.b64decode(
        decoded_payload['kllsketch'].encode('ascii'),
    )
    restored = kll_floats_sketch.deserialize(restored_bytes)

    restored_min = restored.get_min_value()
    restored_max = restored.get_max_value()
    expected_restored_min = sketch.get_min_value()
    expected_restored_max = sketch.get_max_value()
    assert restored_min == expected_restored_min, (
        f'Restored KLL min mismatch: expected {expected_restored_min}, got {restored_min}'
    )
    assert restored_max == expected_restored_max, (
        f'Restored KLL max mismatch: expected {expected_restored_max}, got {restored_max}'
    )
    restored_quantiles = restored.get_quantiles(quantiles)
    assert restored_quantiles == combined_actual, (
        'Restored KLL quantiles mismatch: '
        f'expected {combined_actual}, got {restored_quantiles}'
    )


def test_reqfloatssketch_quantiles_merge_and_serialization():
    quantiles = [0.1, 0.5, 0.9, 0.99]
    base_data = [value * 0.5 for value in range(1, 5_000)]
    secondary_data = [10_000.0 + value * 0.25 for value in range(2_500)]

    sketch = _build_req_floats_sketch(base_data)
    min_value = sketch.get_min_value()
    max_value = sketch.get_max_value()
    assert math.isclose(min_value, min(base_data)), (
        f'Initial REQ float min mismatch: expected {min(base_data)}, got {min_value}'
    )
    assert math.isclose(max_value, max(base_data)), (
        f'Initial REQ float max mismatch: expected {max(base_data)}, got {max_value}'
    )
    assert sketch.n == len(base_data), (
        f'Initial REQ float count mismatch: expected {len(base_data)}, got {sketch.n}'
    )

    expected_quantiles = _expected_quantiles(base_data, quantiles)
    actual_quantiles = sketch.get_quantiles(quantiles)
    abs_tolerance = len(base_data) * 0.05

    for quantile, expected, actual in zip(
        quantiles,
        expected_quantiles,
        actual_quantiles,
    ):
        assert math.isclose(actual, expected, abs_tol=abs_tolerance), (
            f'REQ float quantile {quantile}: expected around {expected} +/- {abs_tolerance}, got {actual}'
        )

    merged_sketch = _build_req_floats_sketch(secondary_data)
    sketch.merge(merged_sketch)

    combined_data = base_data + secondary_data
    combined_expected = _expected_quantiles(combined_data, quantiles)
    combined_actual = sketch.get_quantiles(quantiles)

    assert math.isclose(sketch.get_min_value(), min(combined_data)), (
        f'Merged REQ float min mismatch: expected {min(combined_data)}, got {sketch.get_min_value()}'
    )
    assert math.isclose(sketch.get_max_value(), max(combined_data)), (
        f'Merged REQ float max mismatch: expected {max(combined_data)}, got {sketch.get_max_value()}'
    )
    assert sketch.n == len(combined_data), (
        f'Merged REQ float count mismatch: expected {len(combined_data)}, got {sketch.n}'
    )

    for quantile, expected, actual in zip(
        quantiles,
        combined_expected,
        combined_actual,
    ):
        assert math.isclose(actual, expected, abs_tol=abs_tolerance), (
            f'REQ float post-merge quantile {quantile}: expected around {expected} +/- {abs_tolerance}, got {actual}'
        )

    serialized = sketch.serialize()
    encoded = base64.b64encode(serialized).decode('ascii')
    json_payload = json.dumps({'reqfloatsketch': encoded})

    decoded_payload = json.loads(json_payload)
    restored_bytes = base64.b64decode(
        decoded_payload['reqfloatsketch'].encode('ascii'),
    )
    restored = req_floats_sketch.deserialize(restored_bytes)

    assert math.isclose(restored.get_min_value(), sketch.get_min_value()), (
        f'Restored REQ float min mismatch: expected {sketch.get_min_value()}, got {restored.get_min_value()}'
    )
    assert math.isclose(restored.get_max_value(), sketch.get_max_value()), (
        f'Restored REQ float max mismatch: expected {sketch.get_max_value()}, got {restored.get_max_value()}'
    )
    assert restored.n == sketch.n, (
        f'Restored REQ float count mismatch: expected {sketch.n}, got {restored.n}'
    )

    restored_quantiles = restored.get_quantiles(quantiles)
    assert restored_quantiles == combined_actual, (
        'Restored REQ float quantiles mismatch: '
        f'expected {combined_actual}, got {restored_quantiles}'
    )


def test_tdigest_quantiles_merge_and_serialization():
    quantiles = [0.1, 0.5, 0.9, 0.99]
    base_data = [value * 0.5 for value in range(1, 5_000)]
    secondary_data = [200_000 + value for value in range(2_500)]

    digest = _build_tdigest(base_data)
    min_base = min(base_data)
    max_base = max(base_data)
    assert math.isclose(digest.get_min_value(), min_base), (
        f'Initial TDigest min mismatch: expected {min_base}, got {digest.get_min_value()}'
    )
    assert math.isclose(digest.get_max_value(), max_base), (
        f'Initial TDigest max mismatch: expected {max_base}, got {digest.get_max_value()}'
    )
    assert digest.get_total_weight() == len(base_data), (
        f'Initial TDigest weight mismatch: expected {len(base_data)}, got {digest.get_total_weight()}'
    )

    expected_quantiles = _expected_quantiles(base_data, quantiles)
    actual_quantiles = [digest.get_quantile(q) for q in quantiles]
    base_abs_tolerance = len(base_data) * 0.05

    for quantile, expected, actual in zip(
        quantiles,
        expected_quantiles,
        actual_quantiles,
    ):
        assert math.isclose(actual, expected, abs_tol=base_abs_tolerance), (
            f'TDigest quantile {quantile}: expected around {expected} +/- {base_abs_tolerance}, got {actual}'
        )

    merged_digest = _build_tdigest(secondary_data)
    digest.merge(merged_digest)

    combined_data = base_data + secondary_data
    combined_expected = _expected_quantiles(combined_data, quantiles)
    combined_actual = [digest.get_quantile(q) for q in quantiles]
    combined_min = min(combined_data)
    combined_max = max(combined_data)
    combined_weight = len(combined_data)
    combined_abs_tolerance = combined_weight * 0.05

    assert math.isclose(digest.get_min_value(), combined_min), (
        f'Merged TDigest min mismatch: expected {combined_min}, got {digest.get_min_value()}'
    )
    assert math.isclose(digest.get_max_value(), combined_max), (
        f'Merged TDigest max mismatch: expected {combined_max}, got {digest.get_max_value()}'
    )
    assert digest.get_total_weight() == combined_weight, (
        f'Merged TDigest weight mismatch: expected {combined_weight}, got {digest.get_total_weight()}'
    )

    for quantile, expected, actual in zip(
        quantiles,
        combined_expected,
        combined_actual,
    ):
        assert math.isclose(
            actual,
            expected,
            abs_tol=combined_abs_tolerance,
        ), (
            f'TDigest post-merge quantile {quantile}: expected around {expected} +/- {combined_abs_tolerance}, got {actual}'
        )

    serialized = digest.serialize()
    encoded = base64.b64encode(serialized).decode('ascii')
    json_payload = json.dumps({'tdigest': encoded})

    decoded_payload = json.loads(json_payload)
    restored_bytes = base64.b64decode(
        decoded_payload['tdigest'].encode('ascii'),
    )
    restored = tdigest_float.deserialize(restored_bytes)

    assert math.isclose(restored.get_min_value(), digest.get_min_value()), (
        f'Restored TDigest min mismatch: expected {digest.get_min_value()}, got {restored.get_min_value()}'
    )
    assert math.isclose(restored.get_max_value(), digest.get_max_value()), (
        f'Restored TDigest max mismatch: expected {digest.get_max_value()}, got {restored.get_max_value()}'
    )
    assert restored.get_total_weight() == digest.get_total_weight(), (
        f'Restored TDigest weight mismatch: expected {digest.get_total_weight()}, got {restored.get_total_weight()}'
    )

    restored_quantiles = [restored.get_quantile(q) for q in quantiles]
    for quantile, expected, actual in zip(
        quantiles,
        combined_actual,
        restored_quantiles,
    ):
        assert math.isclose(
            actual,
            expected,
            abs_tol=combined_abs_tolerance,
        ), (
            f'Restored TDigest quantile {quantile}: expected around {expected} +/- {combined_abs_tolerance}, got {actual}'
        )


def test_ddsketch_quantiles_merge_and_serialization():
    quantiles = [0.1, 0.5, 0.9, 0.99]
    base_data = [value * 0.5 for value in range(1, 5_000)]
    secondary_data = [200_000 + value for value in range(2_500)]

    sketch = _build_dd_sketch(base_data)
    min_base = min(base_data)
    max_base = max(base_data)
    len_base = len(base_data)
    assert math.isclose(sketch._min, min_base), (
        f'Initial DDSketch min mismatch: expected {min_base}, got {sketch._min}'
    )
    assert math.isclose(sketch._max, max_base), (
        f'Initial DDSketch max mismatch: expected {max_base}, got {sketch._max}'
    )
    assert math.isclose(sketch.num_values, len_base), (
        f'Initial DDSketch count mismatch: expected {len_base}, got {sketch.num_values}'
    )

    expected_quantiles = _expected_quantiles(base_data, quantiles)
    actual_quantiles = [sketch.get_quantile_value(q) for q in quantiles]

    for quantile, expected, actual in zip(
        quantiles,
        expected_quantiles,
        actual_quantiles,
    ):
        assert math.isclose(actual, expected, rel_tol=0.05), (
            f'DDSketch quantile {quantile}: expected {expected}, got {actual}'
        )

    merged_sketch = _build_dd_sketch(secondary_data)
    sketch.merge(merged_sketch)

    combined_data = base_data + secondary_data
    combined_expected = _expected_quantiles(combined_data, quantiles)
    combined_actual = [sketch.get_quantile_value(q) for q in quantiles]

    combined_min = min(combined_data)
    combined_max = max(combined_data)
    combined_len = len(combined_data)
    combined_sum = sum(combined_data)
    assert math.isclose(sketch._min, combined_min), (
        f'Merged DDSketch min mismatch: expected {combined_min}, got {sketch._min}'
    )
    assert math.isclose(sketch._max, combined_max), (
        f'Merged DDSketch max mismatch: expected {combined_max}, got {sketch._max}'
    )
    assert math.isclose(sketch.num_values, combined_len), (
        f'Merged DDSketch count mismatch: expected {combined_len}, got {sketch.num_values}'
    )
    assert math.isclose(sketch.sum, combined_sum), (
        f'Merged DDSketch sum mismatch: expected {combined_sum}, got {sketch.sum}'
    )

    for quantile, expected, actual in zip(
        quantiles,
        combined_expected,
        combined_actual,
    ):
        assert math.isclose(actual, expected, rel_tol=0.05), (
            f'DDSketch post-merge quantile {quantile}: expected {expected}, got {actual}'
        )

    proto = DDSketchProto.to_proto(sketch)
    serialized = proto.SerializeToString()
    encoded = base64.b64encode(serialized).decode('ascii')
    json_payload = json.dumps({'ddsketch': encoded})

    decoded_payload = json.loads(json_payload)
    restored_bytes = base64.b64decode(
        decoded_payload['ddsketch'].encode('ascii'),
    )

    restored_proto = pb.DDSketch.FromString(restored_bytes)
    restored_sketch = DDSketchProto.from_proto(restored_proto)

    assert restored_sketch.num_values == sketch.num_values, (
        f'Restored DDSketch count mismatch: expected {sketch.num_values}, got {restored_sketch.num_values}'
    )

    restored_quantiles = [
        restored_sketch.get_quantile_value(q) for q in quantiles
    ]
    for quantile, expected, actual in zip(
        quantiles,
        combined_actual,
        restored_quantiles,
    ):
        assert math.isclose(actual, expected, rel_tol=0.01), (
            f'Restored DDSketch quantile {quantile}: expected {expected}, got {actual}'
        )
