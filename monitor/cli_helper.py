"""Helpers for parsing CLI arguments for the Icicle monitor."""

from __future__ import annotations

import argparse
from collections.abc import Callable
from collections.abc import Sequence
from enum import Enum
from typing import TypeVar

from monitor.conf import Config
from monitor.conf import FileSystemType
from monitor.conf import LustreFidResolutionMethod
from monitor.conf import OutputDestination

EnumT = TypeVar('EnumT', bound=Enum)


def _enum_type(enum_cls: type[EnumT]) -> Callable[[str], EnumT]:
    """Return a parser callback that maps CLI strings to Enum values."""

    def convert(value: str) -> EnumT:
        normalized = value.lower()
        for member in enum_cls:
            if (
                member.value.lower() == normalized
                or member.name.lower() == normalized
            ):
                return member
        valid = ', '.join(member.value for member in enum_cls)
        raise argparse.ArgumentTypeError(
            f'Expected one of [{valid}], got {value!r}',
        )

    return convert


def _bool_type(value: str) -> bool:
    """Map a textual representation of a boolean into a bool."""
    truthy = {'1', 'true', 't', 'yes', 'y', 'on'}
    falsy = {'0', 'false', 'f', 'no', 'n', 'off'}
    normalized = value.lower()
    if normalized in truthy:
        return True
    if normalized in falsy:
        return False
    valid = ', '.join(sorted(truthy | falsy))
    raise argparse.ArgumentTypeError(
        f'Invalid boolean value {value!r}. Expected one of [{valid}]',
    )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Build an argument parser for overriding config from the CLI."""
    parser = argparse.ArgumentParser(
        description='Run the Icicle changelog monitor',
    )
    parser.add_argument(
        '--log_level',
        default='INFO',
        help='Python logging level to use (default: INFO)',
    )
    parser.add_argument(
        '--fs_type',
        type=_enum_type(FileSystemType),
        help='Filesystem backend to monitor (ibm|lfs)',
    )
    parser.add_argument(
        '--changelog_mode',
        type=_bool_type,
        help='Enable changelog mode (true/false)',
    )
    parser.add_argument(
        '--print_changelogs',
        type=_bool_type,
        help='Print changelog entries instead of forwarding them',
    )
    parser.add_argument(
        '--enable_reduction_rules',
        type=_bool_type,
        help='Enable batch reduction rules (true/false)',
    )
    parser.add_argument(
        '--output_destination',
        type=_enum_type(OutputDestination),
        help='Destination for processed batches (kafka|JSON|none)',
    )
    parser.add_argument(
        '--output_handle',
        help='Handle or topic name used by the configured destination',
    )
    parser.add_argument(
        '--lustre_cid',
        help='Lustre changelog ID to follow (implies --fs_type lfs)',
    )
    parser.add_argument(
        '--lustre_ignore_events',
        help='Comma-separated Lustre event IDs to skip',
    )
    parser.add_argument(
        '--lustre_mdt',
        help='Name of the Lustre MDT the changelog belongs to',
    )
    parser.add_argument(
        '--lustre_fsname',
        help='Name of the Lustre filesystem',
    )
    parser.add_argument(
        '--lustre_fs_mount_point',
        help='Mount point for the Lustre filesystem',
    )
    parser.add_argument(
        '--lustre_fid_resolution_method',
        type=_enum_type(LustreFidResolutionMethod),
        help='FID resolution method for Lustre (naive|fsmonitor|icicle)',
    )
    parser.add_argument(
        '--lustre_max_batch_size',
        type=int,
        help='Maximum Lustre batch size',
    )
    parser.add_argument(
        '--gpfs_topic',
        help='Kafka topic to consume GPFS changelogs from',
    )
    parser.add_argument(
        '--gpfs_partition',
        type=int,
        help='Kafka partition to consume (-1 = all)',
    )
    parser.add_argument(
        '--gpfs_ignore_events',
        help='Comma-separated GPFS changelog events to skip',
    )
    parser.add_argument(
        '--gpfs_max_batch_size',
        type=int,
        help='Maximum GPFS batch size',
    )
    parser.add_argument(
        '--gpfs_idle_grace_seconds',
        type=float,
        help='Idle grace before GPFS cworkers stop (0 = exit on first empty poll)',  # noqa: E501
    )
    parser.add_argument(
        '--ignore_events',
        help='Shortcut for --lustre_ignore_events or --gpfs_ignore_events depending on fs_type',  # noqa: E501
    )
    return parser.parse_args(argv)


def build_config_from_args(args: argparse.Namespace) -> Config:  # noqa: C901, PLR0912
    """Create a Config object with any CLI overrides applied."""
    config = Config()

    lustre_override = any(
        getattr(
            args,
            name,
            None,
        )
        is not None
        for name in (
            'lustre_cid',
            'lustre_ignore_events',
            'lustre_mdt',
            'lustre_fsname',
            'lustre_fs_mount_point',
            'lustre_fid_resolution_method',
            'lustre_max_batch_size',
        )
    )

    if args.fs_type is not None:
        config.general.fs_type = args.fs_type
    elif lustre_override:
        config.general.fs_type = FileSystemType.Lustre

    if args.changelog_mode is not None:
        config.general.changelog_mode = args.changelog_mode

    if args.print_changelogs is not None:
        config.general.print_changelogs = args.print_changelogs

    if args.enable_reduction_rules is not None:
        config.batch_processor.enable_reduction_rules = (
            args.enable_reduction_rules
        )

    if args.output_destination is not None:
        config.output.destination = args.output_destination

    if args.output_handle is not None:
        config.output.handle = args.output_handle

    if args.lustre_cid is not None:
        config.lustre.cid = args.lustre_cid

    if args.lustre_ignore_events is not None:
        config.lustre.ignore_events = args.lustre_ignore_events

    if args.lustre_mdt is not None:
        config.lustre.mdt = args.lustre_mdt

    if args.lustre_fsname is not None:
        config.lustre.fsname = args.lustre_fsname

    if args.lustre_fs_mount_point is not None:
        config.lustre.fs_mount_point = args.lustre_fs_mount_point

    if args.lustre_fid_resolution_method is not None:
        config.lustre.fid_resolution_method = args.lustre_fid_resolution_method

    if args.lustre_max_batch_size is not None:
        config.lustre.max_batch_size = args.lustre_max_batch_size

    if args.gpfs_topic is not None:
        config.gpfs.topic = args.gpfs_topic

    if args.gpfs_partition is not None:
        config.gpfs.partition = args.gpfs_partition

    if args.gpfs_ignore_events is not None:
        config.gpfs.ignore_events = args.gpfs_ignore_events

    if args.gpfs_max_batch_size is not None:
        config.gpfs.max_batch_size = args.gpfs_max_batch_size

    if args.gpfs_idle_grace_seconds is not None:
        config.gpfs.idle_grace_seconds = args.gpfs_idle_grace_seconds

    if args.ignore_events is not None:
        if config.general.fs_type == FileSystemType.Lustre:
            config.lustre.ignore_events = args.ignore_events
        else:
            config.gpfs.ignore_events = args.ignore_events

    return config
