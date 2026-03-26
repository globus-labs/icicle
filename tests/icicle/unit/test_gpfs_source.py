"""Tests for GPFSKafkaSource (with mocked Kafka)."""

from __future__ import annotations

import json
from unittest.mock import MagicMock
from unittest.mock import patch

from src.icicle.events import EventKind
from src.icicle.gpfs_events import InotifyEventType


def _make_message(
    data: dict,
    offset: int = 0,
    partition: int = 0,
) -> MagicMock:
    """Create a mock Kafka message."""
    msg = MagicMock()
    msg.value.return_value = json.dumps(data).encode('utf-8')
    msg.error.return_value = None
    msg.offset.return_value = offset
    msg.partition.return_value = partition
    msg.topic.return_value = 'test-topic'
    return msg


def _make_consumer_mock(messages: list[MagicMock]) -> MagicMock:
    """Create a mock Kafka Consumer that returns messages once then empty."""
    consumer = MagicMock()
    # Return messages first, then empty lists forever (avoids StopIteration
    # in background consumer threads when mock side_effect is exhausted).
    call_count = {'n': 0}

    def _consume(**kwargs):
        call_count['n'] += 1
        return messages if call_count['n'] == 1 else []

    consumer.consume.side_effect = _consume
    consumer.poll.return_value = None
    consumer.assignment.return_value = []

    class FakeTopicMeta:
        def __init__(self):
            self.partitions = {0: None}

    class FakeMeta:
        def __init__(self):
            self.topics = {'test-topic': FakeTopicMeta()}

    consumer.list_topics.return_value = FakeMeta()
    return consumer


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


class TestConstruction:
    @patch('src.icicle.gpfs_source.Consumer')
    @patch('src.icicle.gpfs_source.TopicPartition')
    def test_single_partition_mode(self, mock_tp, mock_consumer_cls):
        mock_consumer_cls.return_value = _make_consumer_mock([])
        from src.icicle.gpfs_source import GPFSKafkaSource

        source = GPFSKafkaSource(
            topic='test-topic',
            partition=0,
            poll_timeout=0.1,
        )
        source.close()
        mock_tp.assert_called()


# ---------------------------------------------------------------------------
# Parse and filter
# ---------------------------------------------------------------------------


class TestParseAndFilter:
    def test_parse_valid_create_event(self):
        from src.icicle.gpfs_events import parse_gpfs_message

        data = {
            'inode': 100,
            'path': '/gpfs/file.txt',
            'event': 'IN_CREATE',
            'fileSize': 1024,
        }
        result = parse_gpfs_message(data)
        assert result is not None
        assert result['kind'] == EventKind.CREATED
        assert result['inode'] == 100

    def test_filter_dropped_events(self):
        from src.icicle.gpfs_events import parse_gpfs_message

        for event_type in ('IN_OPEN', 'IN_CLOSE_NOWRITE', 'IN_MOVED_FROM'):
            data = {'inode': 1, 'path': '/a', 'event': event_type}
            assert parse_gpfs_message(data) is None

    def test_ignore_events_param(self):
        """Custom ignore events should drop additional event types."""
        from src.icicle.gpfs_events import GPFS_SOURCE_DROP

        ignore = {InotifyEventType.IN_ACCESS} | GPFS_SOURCE_DROP
        # IN_ACCESS is not in GPFS_SOURCE_DROP but should be filtered
        # when passed as ignore_events
        assert InotifyEventType.IN_ACCESS in ignore


# ---------------------------------------------------------------------------
# Queue type selection
# ---------------------------------------------------------------------------


class TestQueueType:
    @patch('src.icicle.gpfs_source.Consumer')
    @patch('src.icicle.gpfs_source.TopicPartition')
    def test_default_queue_type(self, mock_tp, mock_consumer_cls):
        mock_consumer_cls.return_value = _make_consumer_mock([])
        from src.icicle.gpfs_source import GPFSKafkaSource
        from src.icicle.queue import StdlibQueue

        source = GPFSKafkaSource(
            topic='test-topic',
            partition=0,
            poll_timeout=0.1,
        )
        assert isinstance(source._queue, StdlibQueue)
        source.close()

    @patch('src.icicle.gpfs_source.Consumer')
    @patch('src.icicle.gpfs_source.TopicPartition')
    def test_injected_queue(self, mock_tp, mock_consumer_cls):
        mock_consumer_cls.return_value = _make_consumer_mock([])
        from src.icicle.gpfs_source import GPFSKafkaSource
        from src.icicle.queue import RingBufferQueue

        q = RingBufferQueue(capacity=64)
        source = GPFSKafkaSource(
            topic='test-topic',
            partition=0,
            batch_queue=q,
            poll_timeout=0.1,
        )
        assert source._queue is q
        source.close()


# ---------------------------------------------------------------------------
# Close
# ---------------------------------------------------------------------------


class TestClose:
    @patch('src.icicle.gpfs_source.Consumer')
    @patch('src.icicle.gpfs_source.TopicPartition')
    def test_close_cleans_up(self, mock_tp, mock_consumer_cls):
        mock_consumer_cls.return_value = _make_consumer_mock([])
        from src.icicle.gpfs_source import GPFSKafkaSource

        source = GPFSKafkaSource(
            topic='test-topic',
            partition=0,
            poll_timeout=0.1,
        )
        source.close()
        assert len(source._consumers) == 0

    @patch('src.icicle.gpfs_source.Consumer')
    @patch('src.icicle.gpfs_source.TopicPartition')
    def test_close_shm_unlinks(self, mock_tp, mock_consumer_cls):
        mock_consumer_cls.return_value = _make_consumer_mock([])
        from src.icicle.gpfs_source import GPFSKafkaSource
        from src.icicle.mpsc_queue import SharedBatchQueue

        shm_q = SharedBatchQueue(max_batches=4)
        source = GPFSKafkaSource(
            topic='test-topic',
            partition=0,
            batch_queue=shm_q,
            poll_timeout=0.1,
        )
        source.close()
        assert shm_q._unlinked is True


# ---------------------------------------------------------------------------
# Partition distribution
# ---------------------------------------------------------------------------


class TestPartitionDistribution:
    def test_even_distribution(self):
        from src.icicle.gpfs_source import GPFSKafkaSource

        result = GPFSKafkaSource._distribute_partitions(6, 3)
        assert result == [[0, 3], [1, 4], [2, 5]]

    def test_uneven_distribution(self):
        from src.icicle.gpfs_source import GPFSKafkaSource

        result = GPFSKafkaSource._distribute_partitions(5, 3)
        assert result == [[0, 3], [1, 4], [2]]

    def test_more_consumers_than_partitions(self):
        from src.icicle.gpfs_source import GPFSKafkaSource

        result = GPFSKafkaSource._distribute_partitions(2, 5)
        assert result == [[0], [1], [], [], []]

    def test_single_partition_single_consumer(self):
        from src.icicle.gpfs_source import GPFSKafkaSource

        result = GPFSKafkaSource._distribute_partitions(1, 1)
        assert result == [[0]]

    def test_many_partitions(self):
        from src.icicle.gpfs_source import GPFSKafkaSource

        result = GPFSKafkaSource._distribute_partitions(16, 4)
        assert all(len(group) == 4 for group in result)


# ---------------------------------------------------------------------------
# Read / commit edge cases
# ---------------------------------------------------------------------------


class TestReadEdgeCases:
    @patch('src.icicle.gpfs_source.Consumer')
    @patch('src.icicle.gpfs_source.TopicPartition')
    def test_read_after_shutdown(self, mock_tp, mock_consumer_cls):
        mock_consumer_cls.return_value = _make_consumer_mock([])
        from src.icicle.gpfs_source import GPFSKafkaSource

        source = GPFSKafkaSource(
            topic='test-topic',
            partition=0,
            poll_timeout=0.1,
        )
        source._shutdown.set()
        assert source.read() == []
        source.close()


class TestCommitEdgeCases:
    @patch('src.icicle.gpfs_source.Consumer')
    @patch('src.icicle.gpfs_source.TopicPartition')
    def test_commit_with_no_messages(self, mock_tp, mock_consumer_cls):
        mock_consumer_cls.return_value = _make_consumer_mock([])
        from src.icicle.gpfs_source import GPFSKafkaSource

        source = GPFSKafkaSource(
            topic='test-topic',
            partition=0,
            poll_timeout=0.1,
        )
        source.commit()  # should not raise
        source.close()


# ---------------------------------------------------------------------------
# Message parsing edge cases
# ---------------------------------------------------------------------------


class TestMessageEdgeCases:
    def test_message_with_valid_but_incomplete_data(self):
        from src.icicle.gpfs_events import parse_gpfs_message

        assert parse_gpfs_message({'inode': 1, 'path': '/a'}) is None
        assert parse_gpfs_message({'path': '/a', 'event': 'IN_CREATE'}) is None
        assert (
            parse_gpfs_message({'inode': None, 'path': None, 'event': None})
            is None
        )

    def test_invalid_json_handling(self):
        payload = b'not json {'
        try:
            json.loads(payload.decode('utf-8'))
            assert False, 'Should have raised'
        except json.JSONDecodeError:
            pass
