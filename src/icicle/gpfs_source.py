"""GPFS Kafka event source for icicle."""

from __future__ import annotations

import json
import logging
import queue
import threading
from typing import Any

from confluent_kafka import Consumer
from confluent_kafka import KafkaException
from confluent_kafka import TopicPartition

from src.icicle.gpfs_events import GPFS_SOURCE_DROP
from src.icicle.gpfs_events import InotifyEventType
from src.icicle.gpfs_events import parse_gpfs_message
from src.icicle.queue import BatchQueue
from src.icicle.queue import StdlibQueue
from src.icicle.source import EventSource

logger = logging.getLogger(__name__)


class GPFSKafkaSource(EventSource):
    """Reads GPFS inotify events from Kafka (mmwatch topic).

    Supports configurable parallelism via multiple consumer threads
    and pluggable BatchQueue implementations.
    """

    def __init__(  # noqa: PLR0913
        self,
        topic: str,
        bootstrap_servers: str = 'localhost:9092',
        group_id: str = 'icicle-gpfs',
        partition: int = -1,
        num_consumers: int = 1,
        poll_timeout: float = 1.0,
        max_batch_size: int = 500,
        ignore_events: set[InotifyEventType] | None = None,
        batch_queue: BatchQueue | None = None,
        drain: bool = False,
    ) -> None:
        """Initialize the GPFS Kafka event source.

        Args:
            topic: Kafka topic to consume from.
            bootstrap_servers: Kafka bootstrap servers.
            group_id: Consumer group ID.
            partition: Specific partition (-1 = all partitions).
            num_consumers: Number of consumer threads.
            poll_timeout: Kafka poll timeout in seconds.
            max_batch_size: Maximum messages per consume call.
            ignore_events: Additional event types to drop at source.
            batch_queue: Queue implementation. Defaults to StdlibQueue.
            drain: If True, exit after consuming all available messages.
        """
        self._topic = topic
        self._bootstrap_servers = bootstrap_servers
        self._group_id = group_id
        self._partition = partition
        self._num_consumers = num_consumers
        self._poll_timeout = poll_timeout
        self._max_batch_size = max_batch_size
        self._ignore_events = (ignore_events or set()) | GPFS_SOURCE_DROP
        self._drain = drain
        self._shutdown = threading.Event()
        self._consumers: list[Any] = []
        self._last_messages: dict[tuple[str, int], Any] = {}

        self._queue: BatchQueue = (
            batch_queue if batch_queue is not None else StdlibQueue()
        )

        # Counters for stats().
        self._events = 0
        self._ignored = 0
        self._errors = 0

        # Start consumer threads
        self._start_consumers()

    def _make_consumer_config(
        self, group_suffix: str | None = None,
    ) -> dict[str, Any]:
        """Build Kafka consumer config dict."""
        group = (
            f'{self._group_id}-{group_suffix}'
            if group_suffix
            else self._group_id
        )
        return {
            'bootstrap.servers': self._bootstrap_servers,
            'group.id': group,
            'auto.offset.reset': 'latest',
            'enable.auto.commit': False,
        }

    def _start_consumer_thread(self, consumer: Any) -> None:
        """Start a daemon thread running the consumer loop."""
        self._consumers.append(consumer)
        t = threading.Thread(
            target=self._consumer_loop,
            args=(consumer,),
            daemon=True,
        )
        t.start()

    def _start_consumers(self) -> None:
        """Start Kafka consumer threads."""
        if self._partition >= 0:
            consumer = Consumer(self._make_consumer_config())
            consumer.assign(
                [TopicPartition(self._topic, self._partition)],
            )
            self._start_consumer_thread(consumer)
        else:
            # Discover partitions via a temporary consumer.
            tmp = Consumer(self._make_consumer_config())
            tmp.subscribe([self._topic])
            tmp.poll(5.0)
            metadata = tmp.list_topics(self._topic, timeout=10)
            topic_meta = metadata.topics.get(self._topic)
            num_partitions = (
                len(topic_meta.partitions) if topic_meta else 1
            )
            tmp.close()

            n = min(self._num_consumers, num_partitions)
            for i, parts in enumerate(
                self._distribute_partitions(num_partitions, n),
            ):
                consumer = Consumer(
                    self._make_consumer_config(str(i)),
                )
                tps = [TopicPartition(self._topic, p) for p in parts]
                consumer.assign(tps)
                self._start_consumer_thread(consumer)

        logger.info(
            'GPFSKafkaSource started: %d consumer(s), topic=%s',
            len(self._consumers),
            self._topic,
        )

    @staticmethod
    def _distribute_partitions(
        num_partitions: int,
        num_consumers: int,
    ) -> list[list[int]]:
        """Distribute partitions evenly across consumers."""
        result: list[list[int]] = [[] for _ in range(num_consumers)]
        for p in range(num_partitions):
            result[p % num_consumers].append(p)
        return result

    def _process_messages(self, messages: list[Any]) -> list[dict[str, Any]]:
        """Decode and filter a batch of Kafka messages into events."""
        batch: list[dict[str, Any]] = []
        for msg in messages:
            if msg.error():
                logger.warning('message error: %s', msg.error())
                self._errors += 1
                continue

            payload = msg.value()
            if not payload:
                continue

            try:
                data = json.loads(payload.decode('utf-8'))
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                logger.warning('decode error: %s', exc)
                self._errors += 1
                continue

            event = parse_gpfs_message(data)
            if event is None:
                continue

            if event.get('event_type') in self._ignore_events:
                self._ignored += 1
                continue

            event['eid'] = msg.offset()
            batch.append(event)

            key = (msg.topic(), msg.partition())
            self._last_messages[key] = msg
        return batch

    def _consumer_loop(self, consumer: Any) -> None:
        """Worker loop: poll Kafka, push raw events to the queue."""
        try:
            while not self._shutdown.is_set():
                try:
                    messages = consumer.consume(
                        num_messages=self._max_batch_size,
                        timeout=self._poll_timeout,
                    )
                except KafkaException as err:
                    logger.error('consume error: %s', err)
                    continue

                batch = self._process_messages(messages)

                if batch:
                    self._queue.put(batch)
                elif self._drain:
                    break
        finally:
            # Signal end of stream for this consumer
            self._queue.put(None)

    def read(self) -> list[dict[str, Any]]:
        """Read a batch of events from the internal queue.

        Blocks until events are available. Returns empty list when closed.
        """
        if self._shutdown.is_set():
            return []

        try:
            batch = self._queue.get(timeout=self._poll_timeout)
        except queue.Empty:
            return []

        if batch is None:
            return []

        self._events += len(batch)
        return batch

    def commit(self) -> None:
        """Commit Kafka offsets for the most recent batch."""
        if not self._last_messages:
            return

        for consumer in self._consumers:
            offsets = []
            for (topic, partition), msg in self._last_messages.items():
                offsets.append(
                    TopicPartition(topic, partition, msg.offset() + 1),
                )
            if offsets:
                try:
                    consumer.commit(offsets=offsets, asynchronous=True)
                except Exception as exc:
                    logger.error('commit error: %s', exc)

        self._last_messages.clear()

    def stats(self) -> dict[str, Any]:
        """Return source-level counters."""
        result = {
            'source_events': self._events,
            'source_ignored': self._ignored,
            'source_errors': self._errors,
        }
        result.update(self._queue.stats())
        return result

    def close(self) -> None:
        """Shut down all consumer threads and release resources."""
        self._shutdown.set()

        for consumer in self._consumers:
            try:
                consumer.close()
            except Exception as exc:
                logger.warning('consumer close error: %s', exc)

        self._consumers.clear()
        self._queue.close()

        logger.info('GPFSKafkaSource closed.')
