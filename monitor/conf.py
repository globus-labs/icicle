"""Prototype configuration models using dataclasses and StrEnum."""

from __future__ import annotations

import json
import logging
from dataclasses import asdict
from dataclasses import dataclass
from dataclasses import field
from enum import StrEnum
from time import time
from typing import Any

logger = logging.getLogger(__name__)


class FileSystemType(StrEnum):
    """Enumeration for supported filesystem backends."""

    Lustre = 'lfs'
    IBMStorageScale = 'ibm'


class LustreFidResolutionMethod(StrEnum):
    """Enumeration for Lustre FID resolution methods."""

    NAIVE = 'naive'
    FSMONITOR = 'fsmonitor'
    ICICLE = 'icicle'


class OutputDestination(StrEnum):
    """Enumeration for output destinations."""

    NONE = 'none'
    JSON = 'JSON'
    KAFKA = 'kafka'


@dataclass
class GeneralOptions:
    """General configuration options."""

    nproc: int = 1  # new
    changelog_mode: bool = True  # False: metadata update mode
    print_changelogs: bool = False  # True: only print in metadata update mode
    fs_type: FileSystemType = FileSystemType.IBMStorageScale


@dataclass
class BatchProcessorOptions:
    """Batch processor tuning parameters."""

    enable_reduction_rules: bool = False


@dataclass
class LustreOptions:
    """Lustre-specific configuration options."""

    mdt: str = 'exacloud-MDT0000'
    cid: str = 'cl1'
    fsname: str = 'exacloud'
    fs_mount_point: str = '/mnt/exacloud'
    fid_resolution_method: LustreFidResolutionMethod = (
        LustreFidResolutionMethod.NAIVE
    )
    max_batch_size: int = 10_000
    ignore_events: str | None = None

    def __post_init__(self) -> None:
        """Normalize mount point: drop trailing slash."""
        if self.fs_mount_point.endswith('/'):
            self.fs_mount_point = self.fs_mount_point[:-1]


@dataclass
class GpfsOptions:
    """IBM Storage Scale (GPFS) configuration options."""

    topic: str = 'fset1-topic'
    partition: int = -1  # -1: whole topic, >=0 single partition
    poll_timeout: float = 1.0
    # If > 0, a cworker exits only after this many idle seconds.
    # If <= 0, cworker keeps the old behavior and exits on first empty poll.
    # For online benchmarks, set this to exceed expected quiet gaps.
    idle_grace_seconds: float = 0.0
    max_batch_size: int = 10_000
    config_dict: dict[str, Any] = field(
        default_factory=lambda: {
            'bootstrap.servers': '172.31.43.73:9092',
            'group.id': f'g{int(time())}',  # fresh group.id each time
            'enable.auto.commit': True,
            'auto.offset.reset': 'earliest',
        },
    )
    ignore_events: str | None = None


@dataclass
class MSKOptions:
    """AWS MSK producer configuration options."""

    config_dict: dict[str, Any] = field(
        default_factory=lambda: {
            'bootstrap.servers': (
                'b-2-public.diaspora.jdqn8o.c18.kafka.us-east-1.amazonaws.com:9198,'
                'b-1-public.diaspora.jdqn8o.c18.kafka.us-east-1.amazonaws.com:9198'
            ),
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'OAUTHBEARER',
            'linger.ms': 20,
            'acks': 0,
            'compression.type': 'snappy',
            'queue.buffering.max.messages': 10_000_000,
            'queue.buffering.max.kbytes': 104_857_600,
        },
    )


@dataclass
class OutputOptions:
    """Output configuration options."""

    destination: OutputDestination = OutputDestination.KAFKA
    handle: str | None = 'gpfs-mon-out'
    msk: MSKOptions = field(default_factory=MSKOptions)


@dataclass
class Config:
    """Top-level configuration."""

    general: GeneralOptions = field(default_factory=GeneralOptions)
    batch_processor: BatchProcessorOptions = field(
        default_factory=BatchProcessorOptions,
    )
    lustre: LustreOptions = field(default_factory=LustreOptions)
    gpfs: GpfsOptions = field(default_factory=GpfsOptions)
    output: OutputOptions = field(default_factory=OutputOptions)


def print_config(config: Config, logger: logging.Logger) -> None:
    """Emit the important parts of config for inspection."""
    logger.info('======== Monitor Configuration ========')
    for label, value in (
        ('fs_type', config.general.fs_type.value),
        ('changelog_mode', config.general.changelog_mode),
        ('print_changelogs', config.general.print_changelogs),
        (
            'reduction_rules',
            config.batch_processor.enable_reduction_rules,
        ),
        ('output.destination', config.output.destination),
        ('output.handle', config.output.handle),
    ):
        logger.info('%-24s : %s', label, value)

    if config.general.fs_type == FileSystemType.IBMStorageScale:
        for label, value in (
            ('topic', config.gpfs.topic),
            ('partition', str(config.gpfs.partition)),
            ('ignore_events', config.gpfs.ignore_events),
            ('idle_grace_seconds', str(config.gpfs.idle_grace_seconds)),
            ('group.id', str(config.gpfs.config_dict['group.id'])),
        ):
            logger.info('%-24s : %s', label, value)

    if config.general.fs_type == FileSystemType.Lustre:
        for label, value in (
            ('lustre.mdt', config.lustre.mdt),
            ('lustre.fid', config.lustre.fid_resolution_method),
            ('ignore_events', config.lustre.ignore_events),
        ):
            logger.info('%-24s : %s', label, value)
    logger.info('=' * 39)


__all__ = [
    'BatchProcessorOptions',
    'Config',
    'FileSystemType',
    'GeneralOptions',
    'GpfsOptions',
    'LustreFidResolutionMethod',
    'LustreOptions',
    'MSKOptions',
    'OutputDestination',
    'OutputOptions',
    'print_config',
]


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    )

    config = Config()
    logger.info(json.dumps(asdict(config), indent=2, default=str))
    print_config(config, logger)
