"""Integration tests for KafkaOutputHandler with real Kafka in Docker.

Requires Docker. Tests are skipped if Docker is not available.
Run with: pytest tests/icicle/integration/test_kafka.py -v --noconftest
"""

from __future__ import annotations

import json
import subprocess
import time
from pathlib import Path
from typing import Any

import pytest
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient
from confluent_kafka.admin import NewTopic

from src.icicle.output import KafkaOutputHandler

COMPOSE_FILE = Path(__file__).parent / 'docker-compose.yaml'
BOOTSTRAP = 'localhost:9092'
TOPIC = 'icicle-test'


def _docker_available() -> bool:
    try:
        r = subprocess.run(
            ['docker', 'info'],
            check=False,
            capture_output=True,
            timeout=5,
        )
        return r.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def _kafka_ready(timeout: int = 30) -> bool:
    """Poll until Kafka broker responds or timeout."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            admin = AdminClient({'bootstrap.servers': BOOTSTRAP})
            metadata = admin.list_topics(timeout=5)
            if metadata.brokers:
                return True
        except Exception:
            pass
        time.sleep(1)
    return False


def _create_topic(name: str) -> None:
    admin = AdminClient({'bootstrap.servers': BOOTSTRAP})
    fs = admin.create_topics([NewTopic(name, num_partitions=1)])
    for _, f in fs.items():
        try:
            f.result(timeout=10)
        except Exception:
            pass  # topic may already exist


def _consume_all(
    topic: str,
    expected: int,
    timeout: float = 10.0,
) -> list[dict[str, Any]]:
    """Consume expected number of messages from topic."""
    consumer = Consumer(
        {
            'bootstrap.servers': BOOTSTRAP,
            'group.id': f'icicle-test-{time.time()}',
            'auto.offset.reset': 'earliest',
        },
    )
    consumer.subscribe([topic])
    messages: list[dict[str, Any]] = []
    deadline = time.time() + timeout
    while len(messages) < expected and time.time() < deadline:
        msg = consumer.poll(timeout=1.0)
        if msg is None or msg.error():
            continue
        messages.append(json.loads(msg.value().decode()))
    consumer.close()
    return messages


@pytest.fixture(scope='module')
def kafka_cluster():
    """Start Kafka via docker compose, yield, then tear down."""
    if not _docker_available():
        pytest.skip('Docker not available')

    # Start
    subprocess.run(
        ['docker', 'compose', '-f', str(COMPOSE_FILE), 'up', '-d'],
        check=True,
        capture_output=True,
    )

    try:
        if not _kafka_ready(timeout=30):
            pytest.skip('Kafka did not become ready in time')
        yield
    finally:
        # Tear down
        subprocess.run(
            ['docker', 'compose', '-f', str(COMPOSE_FILE), 'down', '-v'],
            check=False,
            capture_output=True,
        )


class TestKafkaIntegration:
    def test_produce_and_consume(self, kafka_cluster):
        """Write events via KafkaOutputHandler, read them back."""
        _create_topic(TOPIC)

        handler = KafkaOutputHandler(
            topic=TOPIC,
            bootstrap_servers=BOOTSTRAP,
        )

        payloads = [
            {
                'op': 'update',
                'path': '/tmp/test.txt',
                'stat': {'size': 42},
            },
            {'op': 'delete', 'path': '/tmp/old.txt'},
            {
                'op': 'update',
                'path': '/tmp/dir',
                'stat': {'size': 64},
            },
        ]
        handler.send(payloads)
        handler.close()

        messages = _consume_all(TOPIC, expected=3)
        assert len(messages) == 3
        assert messages[0]['op'] == 'update'
        assert messages[0]['path'] == '/tmp/test.txt'
        assert messages[1]['op'] == 'delete'
        assert messages[2]['stat']['size'] == 64

    def test_multiple_send_batches(self, kafka_cluster):
        """Multiple send() calls all arrive."""
        topic = f'{TOPIC}-multi'
        _create_topic(topic)

        handler = KafkaOutputHandler(
            topic=topic,
            bootstrap_servers=BOOTSTRAP,
        )
        handler.send([{'op': 'update', 'path': '/a'}])
        handler.send([{'op': 'update', 'path': '/b'}])
        handler.send([{'op': 'update', 'path': '/c'}])
        handler.close()

        messages = _consume_all(topic, expected=3)
        paths = [m['path'] for m in messages]
        assert paths == ['/a', '/b', '/c']

    def test_empty_send(self, kafka_cluster):
        """Empty send does not produce messages."""
        topic = f'{TOPIC}-empty'
        _create_topic(topic)

        handler = KafkaOutputHandler(
            topic=topic,
            bootstrap_servers=BOOTSTRAP,
        )
        handler.send([])
        handler.close()

        messages = _consume_all(topic, expected=0, timeout=3)
        assert len(messages) == 0
