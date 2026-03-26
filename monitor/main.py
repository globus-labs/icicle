"""Entry points for running Icicle changelog monitor."""

from __future__ import annotations

import copy
import logging
from collections.abc import Sequence
from multiprocessing import Process

from monitor.changelog_monitor import GPFSChangelogMonitor
from monitor.changelog_monitor import GPFSRole
from monitor.changelog_monitor import LFSChangelogMonitor
from monitor.cli_helper import build_config_from_args
from monitor.cli_helper import parse_args
from monitor.conf import Config
from monitor.conf import FileSystemType
from monitor.conf import print_config
from monitor.mpsc_queue import SharedBatchQueue


def setup_logging(log_level: str = 'INFO') -> logging.Logger:
    """Configure basic logging that streams messages to stderr."""
    logging.basicConfig(
        level=log_level.upper(),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    )
    logger = logging.getLogger(__name__)
    return logger


def create_and_run_gpfs(
    config: Config,
    role: GPFSRole = GPFSRole.MONITOR,
    channel: SharedBatchQueue | None = None,
) -> None:
    """Create and run a GPFS changelog monitor."""
    monitor = GPFSChangelogMonitor(config, role, channel)
    try:
        monitor.run()
    finally:
        monitor.close()


def create_and_run_lfs(config: Config) -> dict[str, object]:
    """Create and run a Lustre changelog monitor."""
    monitor = LFSChangelogMonitor(config)
    monitor.run()
    return monitor.close()


def run_monitor(logger: logging.Logger, config: Config | None = None) -> None:
    """Create a single-process Icicle monitor."""
    config = config or Config()
    print_config(config, logger)

    if config.general.fs_type == FileSystemType.IBMStorageScale:
        create_and_run_gpfs(config, GPFSRole.MONITOR, None)
    else:
        create_and_run_lfs(config)


def run_workers(
    logger: logging.Logger,
    config: Config | None = None,
    topic: str = 'fset1-topic-1p',
) -> None:
    """Create a multi-process Icicle monitor.

    Let n be the number of topic partitions.
    monitor = n changelog workers + 1 update worker.
    """
    if config is None:
        config = Config()
    config.gpfs.topic = topic
    config.gpfs.partition = int(topic.split('-')[-1][:-1])
    print_config(config, logger)

    mp_queue = SharedBatchQueue(
        max_batches=100,
        max_payload_bytes=1_000_048_576,
    )

    workers: list[Process] = []

    uworker = Process(
        target=create_and_run_gpfs,
        args=(copy.deepcopy(config), GPFSRole.UWORKER, mp_queue),
    )
    workers.append(uworker)

    for p in range(config.gpfs.partition - 1, -1, -1):
        config.gpfs.partition = p
        cworker = Process(
            target=create_and_run_gpfs,
            args=(copy.deepcopy(config), GPFSRole.CWORKER, mp_queue),
        )
        workers.append(cworker)

    for worker in workers:
        worker.start()

    for worker in workers:
        worker.join()

    mp_queue.close()
    mp_queue.unlink()


def main(argv: Sequence[str] | None = None) -> None:
    """Run the monitor depending on queue and partition mode."""
    args = parse_args(argv)
    logger = setup_logging(args.log_level)
    config = build_config_from_args(args)
    topic = config.gpfs.topic or 'fset1-topic-1p'
    run_workers(logger, config, topic=topic)


if __name__ == '__main__':
    main()
