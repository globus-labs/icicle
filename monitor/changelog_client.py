"""Changelog clients for Lustre and IBM Storage Scale."""

from __future__ import annotations

import json
import logging
import subprocess
from abc import ABC
from abc import abstractmethod
from dataclasses import asdict
from datetime import datetime
from time import perf_counter
from time import time
from typing import Any

from confluent_kafka import Consumer
from confluent_kafka import KafkaException
from confluent_kafka import Message
from confluent_kafka import TopicPartition

from monitor.conf import GpfsOptions
from monitor.conf import LustreOptions
from monitor.data_types import EventType
from monitor.data_types import InotifyEventType

logger = logging.getLogger(__name__)


class ChangelogClient(ABC):
    """Abstract class of ChangelogClient."""

    def __init__(self) -> None:
        """Initialize counters."""
        self._polled = 0  # _polled = _errors + _ignored + _emitted
        self._errors = 0
        self._ignored = 0
        self._emitted = 0

    @abstractmethod
    def read(self, max_messages: int | None = None) -> list[dict[str, Any]]:
        """Fetch events from the changelog source."""

    @abstractmethod
    def commit(self) -> None:
        """Commit offsets or mark consumed events."""

    @abstractmethod
    def close(self) -> None:
        """Release underlying resources."""

    def stats_summary(self) -> dict[str, int]:
        """Return ChangelogClient counters."""
        return {
            'cc_polled': self._polled,
            'cc_errors': self._errors,
            'cc_ignored': self._ignored,
            'cc_emitted': self._emitted,
        }


class GPFSChangelogClient(ChangelogClient):
    """Kafka-backed GPFS changelog client supporting multiple partitions."""

    def __init__(self, options: GpfsOptions) -> None:
        """Construct a Kafka consumer pointed at the GPFS mmwatch topic."""
        logger.debug(
            'GPFSChangelogClient init begins %s',
            datetime.fromtimestamp(time()).isoformat(),
        )
        super().__init__()
        self.cfg = options
        self.ignore_events = self._parse_ignore_events(self.cfg.ignore_events)
        self._prefetched_messages: list[Message] = []
        self._last_messages: dict[tuple[str, int], Message] = {}  # topic, pati
        self._consumer: Consumer | None = Consumer(self.cfg.config_dict)
        if self.cfg.partition >= 0:  # the consumer subscribes to a partition
            partition = TopicPartition(self.cfg.topic, self.cfg.partition)
            self._consumer.assign([partition])
        else:  # the consumer subscribes to the whole topic
            self._consumer.subscribe([self.cfg.topic])
        warmup_message = self._consumer.poll(10)  # in seconds
        if warmup_message is not None:
            self._prefetched_messages.append(warmup_message)
        assignments = [
            (partition.topic, partition.partition)
            for partition in self._consumer.assignment()
        ]
        logger.info(f'Assigned partitions: {assignments}')
        logger.debug(
            'GPFSChangelogClient init ends %s',
            datetime.fromtimestamp(time()).isoformat(),
        )

    def _parse_ignore_events(
        self,
        ignore_spec: str | None,
    ) -> set[InotifyEventType]:
        if not ignore_spec:
            return set()

        events: set[InotifyEventType] = set()
        for token in (part.strip() for part in ignore_spec.split(',')):
            if not token:
                continue
            try:
                events.add(InotifyEventType[token])
            except KeyError:
                logger.warning('unknown inotify event %s', token)
        return events

    def read(self, max_messages: int | None = None) -> list[dict[str, Any]]:
        """Poll Kafka and return validated GPFS events."""
        limit = max_messages if max_messages else self.cfg.max_batch_size

        raw_messages: list[Message] = []
        if self._prefetched_messages:
            raw_messages.extend(self._prefetched_messages)
            self._prefetched_messages = []

        if self._consumer is None:
            logger.debug('read skipped: consumer already closed')
            return self._process_raw_messages(raw_messages)

        remaining = limit - len(raw_messages)
        if remaining > 0:
            try:
                raw_messages.extend(
                    self._consumer.consume(
                        num_messages=remaining,
                        timeout=self.cfg.poll_timeout,
                    ),
                )
            except KafkaException as err:
                logger.error('consume error: %s', err)
                return self._process_raw_messages(raw_messages)

        return self._process_raw_messages(raw_messages)

    def _process_raw_messages(
        self,
        raw_messages: list[Message],
    ) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        for message in raw_messages:
            self._polled += 1

            if (
                message.offset() >= 0
                and not self.cfg.config_dict['enable.auto.commit']
            ):
                key = (message.topic(), message.partition())
                self._last_messages[key] = message

            if message.error():
                logger.warning('message error: %s', message.error())
                self._errors += 1
                continue

            if event := self._parse_message(message):
                events.append(event)

        if not raw_messages:
            logger.debug('No messages polled.')

        return events

    def _parse_message(self, message: Message) -> dict[str, Any] | None:
        payload = message.value()

        if not payload:
            logger.warning('skip message: empty payload')
            self._errors += 1
            return None

        try:
            data = json.loads(payload.decode('utf-8'))

            if not data['inode'] or not data['path'] or not data['event']:
                self._errors += 1
                return None

            event_tokens = data['event'].split()
            if 'IN_ISDIR' not in event_tokens:
                data['event'] = InotifyEventType[event_tokens[0]]
                data['is_dir'] = False
            else:
                non_dir_tokens = [
                    token for token in event_tokens if token != 'IN_ISDIR'
                ]
                if not non_dir_tokens:
                    logger.warning('skip message: IN_ISDIR without partner')
                    self._errors += 1
                    return None
                data['event'] = InotifyEventType[non_dir_tokens[0]]
                data['is_dir'] = True

            if data['event'] in self.ignore_events:
                self._ignored += 1
                return None

            event = {
                'eid': message.offset(),  # int
                'inode': int(data.get('inode')),  # int
                'is_dir': data.get('is_dir'),  # bool
                'event_type': data.get('event'),  # InotifyEventType
                'path': data.get('path'),
                'size': data.get('fileSize'),
                'atime': data.get('atime'),
                'ctime': data.get('ctime'),
                'mtime': data.get('mtime'),
                'permissions': data.get('permissions'),
                'uid': data.get('ownerUserId'),
                'gid': data.get('ownerGroupId'),
            }
            self._emitted += 1
            return event

        except Exception as exc:
            logger.warning('skip message: decode error %s', exc)

        self._errors += 1
        return None

    def commit(self) -> None:
        """Commit the offset for the most recent batch."""
        if self.cfg.config_dict['enable.auto.commit']:
            return

        if not self._last_messages:
            logger.debug('skip commit: nothing to do')
            return

        if self._consumer is None:
            logger.debug('skip commit: consumer already closed')
            return

        try:
            offsets = [
                TopicPartition(topic, partition, message.offset())
                for (topic, partition), message in self._last_messages.items()
            ]
            self._consumer.commit(offsets=offsets, asynchronous=True)
            logger.debug('commit offsets %s', offsets)
            self._last_messages.clear()

        except Exception as exc:
            logger.error('commit error: %s', exc)
            raise

    def close(self) -> None:
        """Close the underlying Kafka consumer if open."""
        if self._consumer is None:
            logger.debug('closing consumer skipped: already closed')
            return

        self._consumer.close()
        self._consumer = None
        logger.debug('consumer closed')


class LFSClient:
    """Client for Lustre File System via `lfs` command-line tool."""

    def __init__(self, mdt: str, cid: str, fsname: str, fs_mount_point: str):
        """Initialise the LFS client.

        Args:
            mdt: The MDT name.
            cid: The changelog client ID.
            fsname: The file system name.
            fs_mount_point: The file system mount point.
        """
        self.mdt = mdt
        self.cid = cid
        self.fsname = fsname
        self.fs_mount_point = fs_mount_point
        # self.fid2path_calls: list[str] = []
        self.fid2path_attempts = 0  # is not cleared

    def read(
        self,
        startrec: int | None = None,
        endrec: int | None = None,
    ) -> list[str]:
        """Read changelog for the configured MDT."""
        logger.debug('Running: sudo lfs changelog %s', self.mdt)
        # TODO: for fair comparison, each algo read all changelogs
        # if startrec and endrec:
        #     completed = subprocess.run(
        #         [
        #             'sudo',
        #             'lfs',
        #             'changelog',
        #             self.mdt,
        #             str(startrec),
        #             str(endrec),
        #         ],
        #         capture_output=True,
        #         text=True,
        #         check=True,
        #     )
        if startrec:
            completed = subprocess.run(
                ['sudo', 'lfs', 'changelog', self.mdt, str(startrec)],
                capture_output=True,
                text=True,
                check=True,
            )
        else:
            completed = subprocess.run(
                ['sudo', 'lfs', 'changelog', self.mdt],
                capture_output=True,
                text=True,
                check=True,
            )
        return completed.stdout.splitlines()

    def changelog_clear(self, offset: str) -> None:
        """Clear changelog up to offset for the configured consumer ID."""
        logger.debug(
            'Committing offset %s for consumer %s on %s',
            offset,
            self.cid,
            self.mdt,
        )
        subprocess.run(
            ['sudo', 'lfs', 'changelog_clear', self.mdt, self.cid, offset],
            check=True,
        )

    def fid2path(self, fid: str) -> str | None:
        """Resolve Lustre FID to full file system path."""
        try:
            start, elapse_ms_report_threshold = perf_counter(), 10
            self.fid2path_attempts += 1
            logger.debug(
                f'fid2path attempt {self.fid2path_attempts} on fid {fid}',
            )
            completed = subprocess.run(
                ['sudo', 'lfs', 'fid2path', self.fsname, fid],
                capture_output=True,
                text=True,
                check=True,
            )
            relative_path = completed.stdout.strip()
            # self.fid2path_calls.append(fid)

            if not relative_path:
                full_path = None
            elif relative_path == '/':
                full_path = f'{self.fs_mount_point}/'
            else:
                full_path = f'{self.fs_mount_point}/{relative_path}'

            elapsed_ms = (perf_counter() - start) * 1000
            if elapsed_ms > elapse_ms_report_threshold:
                # use debug because FileBench + fsmon emitting too many
                logger.debug('Resolving FID %s took %.3f ms', fid, elapsed_ms)

            return full_path

        except subprocess.CalledProcessError:
            return None


def strip_brackets(token: str) -> str:
    """Remove outer brackets from token."""
    if token.startswith('[') and token.endswith(']'):
        return token[1:-1]
    return token


def parse_changelog_line(line: str) -> dict[str, Any] | None:
    """Parse a single changelog line into a plain dict."""
    fields = line.strip().split()
    if len(fields) < 9:  # noqa: PLR2004
        logger.warning(f'Invalid changelog line (too few fields): {line}')
        return None

    try:
        # Base dictionary
        event: dict[str, Any] = {
            'eid': int(fields[0]),
            # 'inode'
            # 'is_dir'
            'event_type': EventType(fields[1]),
            # 'timestamp': fields[2],
            # 'datestamp': fields[3],
            # 'flags': int(fields[4], 16),
            # 'target_fid': None,
            # 'extra_flags': '',
            # 'uid_gid': '',
            # 'nid': '',
        }

        # Parse target FID
        if fields[5].startswith('t='):
            event['target_fid'] = strip_brackets(fields[5][2:])

        # Parse fields 6-8
        # event['extra_flags'] = fields[6].split('=', 1)[1]
        # event['uid_gid'] = fields[7].split('=', 1)[1]
        uid, gid = fields[7].split('=', 1)[1].split(':')
        event['uid'] = uid
        event['gid'] = gid
        # event['nid'] = fields[8].split('=', 1)[1]

        etype = EventType(fields[1])

        # --- Event-specific parsing ---
        if etype in (
            EventType.CREAT,
            EventType.MKDIR,
            EventType.UNLNK,
            EventType.RMDIR,
        ):
            event['parent_fid'] = strip_brackets(fields[9][2:])
            event['name'] = fields[10]

        elif etype == EventType.RENME:
            event['parent_fid'] = strip_brackets(fields[9][2:])
            event['name'] = fields[10]
            event['source_fid'] = strip_brackets(fields[11][2:])
            event['source_parent_fid'] = strip_brackets(fields[12][3:])
            event['old_name'] = fields[13]

        elif etype == EventType.OPEN:
            event['mode'] = fields[9].split('=', 1)[1]  # TODO: permissions?

        return event

    except Exception:
        logger.exception(f'Failed to parse changelog line: {line}')
        return None

    except (ValueError, KeyError) as e:
        logger.error(f'Failed to parse changelog line: {line}, error: {e}')
        return None


def parse_changelog_lines(lines: list[str]) -> list[dict[str, Any]]:
    """Parse a list of changelogs into a list of ChangelogEvent."""
    events: list[dict[str, Any]] = []
    for raw in lines:
        line = raw.strip()
        if not line:
            continue
        ev = parse_changelog_line(line)
        if ev:
            events.append(ev)
    return events


class LFSChangelogClient(ChangelogClient):
    """Lustre changelog client."""

    def __init__(self, options: LustreOptions) -> None:
        logger.debug(
            'LFSChangelogClient init ends %s',
            datetime.fromtimestamp(time()).isoformat(),
        )
        super().__init__()
        self.cfg = options
        self.ignore_events = self._parse_ignore_events(self.cfg.ignore_events)
        self._last_messages: dict[tuple[str, str], int] = {}  # key = mdt, cid
        self._consumer: LFSClient | None = LFSClient(
            options.mdt,
            options.cid,
            options.fsname,
            options.fs_mount_point,
        )
        logger.info(f'Assigned mdt: {options.mdt}, cid: {options.cid}')
        logger.debug(
            'LFSChangelogClient init ends %s',
            datetime.fromtimestamp(time()).isoformat(),
        )

    def _parse_ignore_events(
        self,
        ignore_spec: str | None,
    ) -> set[EventType]:
        if not ignore_spec:
            return set()

        events: set[EventType] = set()
        for token in (part.strip() for part in ignore_spec.split(',')):
            if not token:
                continue
            try:
                events.add(EventType(token))  # TODO: lookup by value
            except KeyError:
                logger.warning('unknown inotify event %s', token)
        return events

    def read(self, max_messages: int | None = None) -> list[dict[str, Any]]:
        assert self._consumer
        incr = 100_000  # TODO: max batch size
        if (self._consumer.mdt, self._consumer.cid) in self._last_messages:
            last = self._last_messages[
                (self._consumer.mdt, self._consumer.cid)
            ]
            lines = self._consumer.read(last + 1, last + incr)

        else:
            # logically should -1, but I get the batch size to be 99999
            start = 1638123  # TODOXX
            lines = self._consumer.read(start, start + incr)
        events = parse_changelog_lines(lines)
        self._polled += len(events)  # TODO

        if events:
            self._last_messages[(self._consumer.mdt, self._consumer.cid)] = (
                events[-1]['eid']
            )

        results = []
        for event in events:
            if event['event_type'] in self.ignore_events:
                self._ignored += 1
                continue
            self._emitted += 1
            results.append(event)
        return results

    def commit(self) -> None:
        """Commit the offset for the most recent batch."""
        if not self._last_messages:
            logger.debug('skip commit: nothing to do')
            return

        if self._consumer is None:
            logger.debug('skip commit: consumer already closed')
            return

        self._consumer.changelog_clear(
            str(self._last_messages[(self._consumer.mdt, self._consumer.cid)]),
        )

    def close(self) -> None:
        """Close the underlying Kafka consumer if open."""
        if self._consumer is None:
            logger.debug('closing consumer skipped: already closed')
            return

        self._consumer = None
        logger.info('consumer closed')


__all__ = ['ChangelogClient', 'GPFSChangelogClient']


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    )
    options = GpfsOptions(topic='fset1-topic', ignore_events='IN_ATTRIB')
    logger.info(json.dumps(asdict(options), indent=2, default=str))

    client = GPFSChangelogClient(options)
    events = client.read()
    client.close()
    logger.info(client.stats_summary())

    # options = LustreOptions(ignore_events='10OPEN')
    # logger.info(json.dumps(asdict(options), indent=2, default=str))

    # client = LFSChangelogClient(options)
    # events = client.read()
    # print(client._last_messages)
    # logger.info(client.stats_summary())
