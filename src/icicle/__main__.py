"""CLI entry point for icicle filesystem monitor."""

from __future__ import annotations

import argparse
import logging
import sys
from typing import Any

from src.icicle.batch import BatchProcessor
from src.icicle.batch import ReductionAction
from src.icicle.events import EventKind
from src.icicle.fswatch_events import STAT_REDUCTION_RULES
from src.icicle.fswatch_source import FSWatchSource
from src.icicle.gpfs_events import GPFS_REDUCTION_RULES
from src.icicle.gpfs_events import InotifyEventType
from src.icicle.gpfs_source import GPFSKafkaSource
from src.icicle.gpfs_state import GPFSStateManager
from src.icicle.lustre_events import LustreEventType
from src.icicle.lustre_source import LustreChangelogSource
from src.icicle.monitor import Monitor
from src.icicle.output import _DIASPORA_BOOTSTRAP_SERVERS
from src.icicle.output import _msk_oauth_cb
from src.icicle.output import _MSK_PRODUCER_DEFAULTS
from src.icicle.output import JsonFileOutputHandler
from src.icicle.output import KafkaOutputHandler
from src.icicle.output import OutputHandler
from src.icicle.output import StdoutOutputHandler
from src.icicle.queue import BatchQueue
from src.icicle.queue import RingBufferQueue
from src.icicle.queue import StdlibQueue
from src.icicle.source import EventSource
from src.icicle.state import StateManager

logger = logging.getLogger(__name__)


def _parse_gpfs_ignore_events(
    raw: str | None,
) -> set[InotifyEventType]:
    """Parse comma-separated inotify event names."""
    result: set[InotifyEventType] = set()
    if not raw:
        return result
    for token in raw.split(','):
        cleaned = token.strip()
        if cleaned:
            try:
                result.add(InotifyEventType[cleaned])
            except KeyError:
                logger.warning('unknown inotify event: %s', cleaned)
    return result


def _parse_lustre_ignore_events(
    raw: str | None,
) -> frozenset[LustreEventType] | None:
    """Parse comma-separated Lustre event type names.

    Accepts short names (e.g. OPEN) or full values (e.g. 10OPEN).
    """
    if not raw:
        return None
    # Build lookup: short name -> enum member (e.g. 'OPEN' -> '10OPEN').
    _by_name = {m.name: m for m in LustreEventType}
    result: set[LustreEventType] = set()
    for token in raw.split(','):
        cleaned = token.strip()
        if not cleaned:
            continue
        if cleaned in _by_name:
            result.add(_by_name[cleaned])
        else:
            try:
                result.add(LustreEventType(cleaned))
            except ValueError:
                logger.warning(
                    'unknown Lustre event type: %s',
                    cleaned,
                )
    return frozenset(result) if result else None


def _parse_fswatch_ignore_flags(raw: str | None) -> frozenset[str] | None:
    """Parse comma-separated fswatch flag names."""
    if not raw:
        return None
    flags = frozenset(t.strip() for t in raw.split(',') if t.strip())
    return flags if flags else None


def _add_output_args(parser: argparse.ArgumentParser) -> None:
    """Add shared output arguments to a parser."""
    parser.add_argument(
        '-o',
        '--json',
        default=None,
        help='output JSON file path',
    )
    parser.add_argument(
        '--kafka',
        metavar='TOPIC',
        default=None,
        help='send output to Kafka topic',
    )
    parser.add_argument(
        '--kafka-bootstrap',
        default='localhost:9092',
        help='Kafka bootstrap servers (default: localhost:9092)',
    )
    parser.add_argument(
        '--kafka-msk',
        action='store_true',
        default=False,
        help='enable AWS MSK IAM OAUTHBEARER auth '
        '(uses diaspora brokers by default)',
    )
    parser.add_argument(
        '-v',
        '--verbose',
        action='store_true',
        help='enable debug logging',
    )
    parser.add_argument(
        '--changelog-mode',
        action='store_true',
        help='output raw events directly, bypassing state processing',
    )
    parser.add_argument(
        '--reduction-rules',
        action='store_true',
        help='enable batch reduction rules '
        '(auto-selected per backend: stat for fswatch/lustre, '
        'gpfs for gpfs)',
    )
    parser.add_argument(
        '--drain',
        action='store_true',
        help='process all available events then exit '
        '(instead of polling for new ones)',
    )
    parser.add_argument(
        '-q',
        '--quiet',
        action='store_true',
        help='suppress event output (stats still logged via -v)',
    )


def _build_output(args: argparse.Namespace) -> OutputHandler | None:
    """Build the output handler from CLI args."""
    if args.quiet:
        if args.kafka:
            logger.info('--quiet: Kafka output to %s disabled', args.kafka)
        if args.json:
            logger.info('--quiet: JSON output to %s disabled', args.json)
        return None
    if args.kafka:
        producer_config: dict[str, Any] | None = None
        bootstrap = args.kafka_bootstrap
        if args.kafka_msk:
            producer_config = {
                **_MSK_PRODUCER_DEFAULTS,
                'oauth_cb': _msk_oauth_cb,
            }
            if bootstrap == 'localhost:9092':
                bootstrap = _DIASPORA_BOOTSTRAP_SERVERS
        return KafkaOutputHandler(
            topic=args.kafka,
            bootstrap_servers=bootstrap,
            producer_config=producer_config,
        )
    if args.json:
        return JsonFileOutputHandler(args.json)
    return StdoutOutputHandler()


def _resolve_reduction_rules(
    enabled: bool,
    command: str,
) -> dict[EventKind, tuple[ReductionAction, set[EventKind]]] | None:
    """Return the backend-appropriate rules if enabled, else None."""
    if not enabled:
        return None
    if command == 'gpfs':
        return GPFS_REDUCTION_RULES
    return STAT_REDUCTION_RULES


def _build_queue(queue_type: str) -> BatchQueue:
    """Build a batch queue from the CLI queue-type choice."""
    if queue_type == 'ring':
        return RingBufferQueue(capacity=128)
    if queue_type == 'shm':
        from src.icicle.mpsc_queue import SharedBatchQueue  # noqa: PLC0415

        return SharedBatchQueue(max_batches=128)
    return StdlibQueue(maxsize=128)


def _apply_yaml_defaults(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
    config: dict[str, Any],
    command: str | None,
) -> None:
    """Set argparse defaults from YAML config values."""
    if not command or command not in subparsers.choices:
        return
    subparser = subparsers.choices[command]
    # Shared (top-level) keys.
    shared_keys = {
        'verbose',
        'changelog_mode',
        'json',
        'kafka',
        'kafka_bootstrap',
        'kafka_msk',
        'reduction_rules',
    }
    defaults: dict[str, Any] = {}
    for key in shared_keys:
        yaml_key = key.replace('_', '-')
        if yaml_key in config:
            defaults[key] = config[yaml_key]
    # Subcommand-specific keys.
    sub = config.get(command)
    if isinstance(sub, dict):
        for yaml_key, value in sub.items():
            defaults[yaml_key.replace('-', '_')] = value
    if defaults:
        subparser.set_defaults(**defaults)


def _build_pipeline(
    args: argparse.Namespace,
) -> tuple[
    EventSource,
    BatchProcessor,
    StateManager | GPFSStateManager | None,
]:
    """Build source, batch processor, and state manager from CLI args."""
    reduction_rules = _resolve_reduction_rules(
        args.reduction_rules,
        args.command,
    )
    changelog_mode: bool = args.changelog_mode

    if args.command == 'fswatch':
        source: EventSource = FSWatchSource(
            args.watch_dir,
            latency=args.latency,
            ignore_flags=_parse_fswatch_ignore_flags(
                args.ignore_events,
            ),
            max_batch_size=args.max_batch_size,
            drain=args.drain,
        )
        batch = BatchProcessor(rules=reduction_rules)
        state: StateManager | GPFSStateManager | None = (
            None if changelog_mode else StateManager()
        )
    elif args.command == 'lustre':
        source = LustreChangelogSource(
            mdt=args.mdt,
            mount_point=args.mount,
            fsname=args.fsname,
            poll_interval=args.poll_interval,
            startrec=args.startrec,
            fid_resolution_method=args.fid_resolution_method,
            ignore_events=_parse_lustre_ignore_events(
                args.ignore_events,
            ),
            max_batch_size=args.max_batch_size,
            drain=args.drain,
        )
        batch = BatchProcessor(rules=reduction_rules)
        state = None if changelog_mode else StateManager()
    else:  # gpfs
        source = GPFSKafkaSource(
            topic=args.topic,
            bootstrap_servers=args.bootstrap,
            group_id=args.group_id,
            partition=args.partition,
            num_consumers=args.num_consumers,
            poll_timeout=args.poll_timeout,
            max_batch_size=args.max_batch_size,
            ignore_events=_parse_gpfs_ignore_events(
                args.ignore_events,
            ),
            batch_queue=_build_queue(args.queue_type),
            drain=args.drain,
        )
        batch = BatchProcessor(
            rules=reduction_rules,
            slot_key='inode',
            bypass_rename=False,
        )
        state = None if changelog_mode else GPFSStateManager()

    return source, batch, state


def main(argv: list[str] | None = None) -> None:
    """Run the icicle filesystem monitor."""
    parser = argparse.ArgumentParser(
        description='icicle — real-time filesystem metadata monitor',
    )
    parser.add_argument(
        '-c',
        '--config',
        default=None,
        help='YAML config file (CLI args override YAML values)',
    )
    subparsers = parser.add_subparsers(dest='command', required=True)

    # --- fswatch subcommand ---
    fswatch_parser = subparsers.add_parser(
        'fswatch',
        help='monitor via fswatch (macOS/Linux)',
    )
    fswatch_parser.add_argument(
        'watch_dir',
        help='directory to watch',
    )
    fswatch_parser.add_argument(
        '-l',
        '--latency',
        type=float,
        default=0.1,
        help='fswatch latency in seconds (default: 0.1)',
    )
    fswatch_parser.add_argument(
        '--ignore-events',
        default=None,
        help='comma-separated fswatch flags to ignore '
        '(e.g. Updated,AttributeModified)',
    )
    fswatch_parser.add_argument(
        '--max-batch-size',
        type=int,
        default=10_000,
        help='max events per read cycle (default: 10000)',
    )
    _add_output_args(fswatch_parser)

    # --- lustre subcommand ---
    lustre_parser = subparsers.add_parser(
        'lustre',
        help='monitor via Lustre changelogs',
    )
    lustre_parser.add_argument(
        '--mdt',
        required=True,
        help='MDT name (e.g. fs0-MDT0000)',
    )
    lustre_parser.add_argument(
        '--mount',
        required=True,
        help='filesystem mount point (e.g. /mnt/fs0)',
    )
    lustre_parser.add_argument(
        '--fsname',
        required=True,
        help='Lustre filesystem name (e.g. fs0)',
    )
    lustre_parser.add_argument(
        '--startrec',
        type=int,
        default=0,
        help='first changelog record to read (default: 0)',
    )
    lustre_parser.add_argument(
        '--poll-interval',
        type=float,
        default=1.0,
        help='poll interval in seconds (default: 1.0)',
    )
    lustre_parser.add_argument(
        '--fid-resolution-method',
        choices=['naive', 'fsmonitor', 'icicle'],
        default='icicle',
        help='FID resolution strategy: naive (always fid2path), '
        'fsmonitor (cache + fid2path), icicle (in-memory paths)',
    )
    lustre_parser.add_argument(
        '--ignore-events',
        default=None,
        help='comma-separated Lustre event types to ignore (e.g. OPEN,CLOSE)',
    )
    lustre_parser.add_argument(
        '--max-batch-size',
        type=int,
        default=100_000,
        help='max changelog records per read cycle (default: 100000)',
    )
    _add_output_args(lustre_parser)

    # --- gpfs subcommand ---
    gpfs_parser = subparsers.add_parser(
        'gpfs',
        help='monitor via GPFS mmwatch Kafka events',
    )
    gpfs_parser.add_argument(
        '--topic',
        required=True,
        help='Kafka topic for mmwatch events',
    )
    gpfs_parser.add_argument(
        '--bootstrap',
        default='localhost:9092',
        help='Kafka bootstrap servers (default: localhost:9092)',
    )
    gpfs_parser.add_argument(
        '--group-id',
        default='icicle-gpfs',
        help='Kafka consumer group ID (default: icicle-gpfs)',
    )
    gpfs_parser.add_argument(
        '--partition',
        type=int,
        default=-1,
        help='specific partition to consume (-1 = all, default: -1)',
    )
    gpfs_parser.add_argument(
        '--num-consumers',
        type=int,
        default=1,
        help='number of consumer threads (default: 1)',
    )
    gpfs_parser.add_argument(
        '--poll-timeout',
        type=float,
        default=1.0,
        help='Kafka poll timeout in seconds (default: 1.0)',
    )
    gpfs_parser.add_argument(
        '--max-batch-size',
        type=int,
        default=500,
        help='max messages per consume call (default: 500)',
    )
    gpfs_parser.add_argument(
        '--ignore-events',
        default=None,
        help='comma-separated inotify event types to ignore (e.g. IN_ACCESS)',
    )
    gpfs_parser.add_argument(
        '--queue-type',
        choices=['stdlib', 'ring', 'shm'],
        default='stdlib',
        help='batch queue implementation (default: stdlib)',
    )
    _add_output_args(gpfs_parser)

    # Two-phase parsing: load YAML config as defaults, then CLI overrides.
    pre_args, _ = parser.parse_known_args(argv)
    if pre_args.config:
        from src.icicle.config import load_config  # noqa: PLC0415

        yaml_config = load_config(pre_args.config)
        _apply_yaml_defaults(subparsers, yaml_config, pre_args.command)

    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        stream=sys.stderr,
    )

    source, batch, state = _build_pipeline(args)
    output = _build_output(args)
    monitor = Monitor(
        source,
        output,
        batch=batch,
        state=state,
        changelog_mode=args.changelog_mode,
    )
    monitor.run()

    stats = monitor.stats()
    formatted = ', '.join(f'{k}, {v}' for k, v in stats.items())
    logger.info('Final stats: %s', formatted)


if __name__ == '__main__':
    main()
