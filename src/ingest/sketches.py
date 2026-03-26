"""Sketch initialization, decoding, and statistics computation."""

from __future__ import annotations

import base64
import datetime
from collections.abc import Callable
from typing import Any

from conf import CMode
from constants import QUANTILE_BREAKPOINTS
from datasketches import kll_floats_sketch
from datasketches import req_floats_sketch
from datasketches import tdigest_float
from ddsketch import DDSketch
from ddsketch.pb import ddsketch_pb2 as pb
from ddsketch.pb.proto import DDSketchProto

_SKETCH_FACTORIES: dict[CMode, Callable[[], Any]] = {
    CMode.dd: DDSketch,
    CMode.kll: kll_floats_sketch,
    CMode.req: req_floats_sketch,
    CMode.td: tdigest_float,
}


def init_metadata_entry(mode: CMode) -> list[Any]:
    """Create a fresh metadata entry: 4 metrics + file count."""
    try:
        sketch_factory = _SKETCH_FACTORIES[mode]
    except KeyError as exc:
        raise ValueError(f'Unsupported mode: {mode}') from exc

    def _init_metric() -> list[Any]:
        return [sketch_factory(), float('inf'), -float('inf'), 0]

    return [_init_metric() for _ in range(4)] + [0]


def decode_sketch(mode: CMode, payload: str) -> Any:
    """Decode a base64-encoded sketch back to its native type."""
    raw = base64.b64decode(payload.encode('ascii'))
    if mode == CMode.dd:
        proto = pb.DDSketch.FromString(raw)
        return DDSketchProto.from_proto(proto)
    if mode == CMode.kll:
        return kll_floats_sketch.deserialize(raw)
    if mode == CMode.req:
        return req_floats_sketch.deserialize(raw)
    if mode == CMode.td:
        return tdigest_float.deserialize(raw)
    raise ValueError(f'Unsupported mode: {mode}')


def encode_sketch(mode: CMode, sketch: Any) -> str:
    """Encode a sketch to a base64 string."""
    if mode == CMode.dd:
        msg = DDSketchProto.to_proto(sketch).SerializeToString()
    else:
        msg = sketch.serialize()
    return base64.b64encode(msg).decode('ascii')


def get_quantile_fn(mode: CMode) -> Callable[..., float]:
    """Return the quantile accessor for the given sketch mode."""
    if mode == CMode.dd:
        return lambda sketch, q: sketch.get_quantile_value(q)
    return lambda sketch, q: sketch.get_quantile(q)


# -- Statistics formatting -------------------------------------------


def _return_stats(
    stats: dict[str, Any],
    metric_map: list[tuple[str, str]],
    value_formatter: Callable[[Any], Any],
    raw_formatter: Callable[[Any], Any] = lambda v: v,
) -> dict[str, Any]:
    response: dict[str, Any] = {'_raw': {}}
    for stat_key, result_key in metric_map:
        raw_value = raw_formatter(stats[stat_key])
        response[result_key] = value_formatter(raw_value)
        response['_raw'][result_key] = raw_value
    return response


def _return_file_stats(
    stats: dict[str, Any],
    prefix: str = 'file_size',
    suffix: str = '_in_bytes',
) -> dict[str, Any]:
    metric_map: list[tuple[str, str]] = [('count', 'file_count')]
    metric_map += [
        (m, f'{prefix}_{m}{suffix}')
        for m in (
            'total',
            'average',
            '10p',
            '25p',
            'median',
            '75p',
            '90p',
            '99p',
            'min',
            'max',
        )
    ]
    return _return_stats(
        stats,
        metric_map,
        value_formatter=lambda v: str(int(v)),
        raw_formatter=int,
    )


def _return_time_stats(
    stats: dict[str, Any],
    prefix: str,
) -> dict[str, Any]:
    def _format_time(ts: Any) -> str:
        dt = datetime.datetime.fromtimestamp(
            int(ts),
            datetime.UTC,
        )
        return dt.strftime('%Y-%m-%d %H:%M:%S') + ' +00:00'

    metric_map = [
        (m, f'{prefix}_{m}')
        for m in (
            'average',
            '10p',
            '25p',
            'median',
            '75p',
            '90p',
            '99p',
            'min',
            'max',
        )
    ]
    return _return_stats(
        stats,
        metric_map,
        value_formatter=_format_time,
        raw_formatter=int,
    )


def calc_file_stats(
    entry: list[Any],
    count: int,
    mode: CMode,
    prefix: str = 'file_size',
    suffix: str = '_in_bytes',
) -> dict[str, Any]:
    """Compute file-size statistics from a sketch entry."""
    sketch, val_min, val_max, val_total = entry
    get_q = get_quantile_fn(mode)
    quantiles = {
        label: int(get_q(sketch, q)) for label, q in QUANTILE_BREAKPOINTS
    }
    count = int(count)
    val_total = int(val_total)
    stats = {
        'count': count,
        'total': val_total,
        'average': val_total // count,
        'min': int(val_min),
        'max': int(val_max),
        **quantiles,
    }
    return _return_file_stats(stats, prefix, suffix)


def calc_time_stats(
    entry: list[Any],
    count: int,
    mode: CMode,
    prefix: str,
) -> dict[str, Any]:
    """Compute time statistics from a sketch entry."""
    sketch, val_min, val_max, val_total = entry
    get_q = get_quantile_fn(mode)
    quantiles = {
        label: int(get_q(sketch, q)) for label, q in QUANTILE_BREAKPOINTS
    }
    count = int(count)
    val_total = int(val_total)
    stats = {
        'average': val_total // count,
        'min': int(val_min),
        'max': int(val_max),
        **quantiles,
    }
    return _return_time_stats(stats, prefix)
