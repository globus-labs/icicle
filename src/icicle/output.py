"""Output handlers for icicle."""

from __future__ import annotations

import json
import logging
import sys
from abc import ABC
from abc import abstractmethod
from pathlib import Path
from typing import Any
from typing import TextIO

logger = logging.getLogger(__name__)


class OutputHandler(ABC):
    """Base class for emitting processed metadata updates."""

    @abstractmethod
    def send(self, payloads: list[dict[str, Any]]) -> None:
        """Send a batch of payloads."""

    @abstractmethod
    def flush(self) -> None:
        """Flush buffered output."""

    @abstractmethod
    def close(self) -> None:
        """Release resources."""

    def __enter__(self) -> OutputHandler:
        """Enter context manager."""
        return self

    def __exit__(self, *exc: Any) -> None:
        """Exit context manager, closing the handler."""
        self.close()


class StdoutOutputHandler(OutputHandler):
    """Writes JSON lines to stdout (default handler)."""

    def send(self, payloads: list[dict[str, Any]]) -> None:
        """Write JSON lines to stdout."""
        for payload in payloads:
            print(json.dumps(payload))
        sys.stdout.flush()

    def flush(self) -> None:
        """Flush stdout."""
        sys.stdout.flush()

    def close(self) -> None:
        """No-op for stdout."""


class JsonFileOutputHandler(OutputHandler):
    """Writes events as a JSON array to a file."""

    def __init__(self, path: str) -> None:
        """Open file and start JSON array."""
        self._path = Path(path)
        self._f: TextIO = self._path.open('w', encoding='utf-8')
        self._first = True
        self._f.write('[\n')

    def send(self, payloads: list[dict[str, Any]]) -> None:
        """Append payloads to the JSON array."""
        for payload in payloads:
            if not self._first:
                self._f.write(',\n')
            json.dump(payload, self._f)
            self._first = False
        if payloads:
            self._f.flush()

    def flush(self) -> None:
        """Flush the file buffer."""
        if self._f and not self._f.closed:
            self._f.flush()

    def close(self) -> None:
        """Close the JSON array and file."""
        if self._f and not self._f.closed:
            self._f.write('\n]\n')
            self._f.close()


_DIASPORA_BOOTSTRAP_SERVERS = (
    'b-1-public.diaspora.jdqn8o.c18.kafka.us-east-1.amazonaws.com:9198,'
    'b-2-public.diaspora.jdqn8o.c18.kafka.us-east-1.amazonaws.com:9198'
)

_MSK_PRODUCER_DEFAULTS: dict[str, Any] = {
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'OAUTHBEARER',
    'linger.ms': 20,
    'acks': 0,
    'compression.type': 'snappy',
    'queue.buffering.max.messages': 10_000_000,
    'queue.buffering.max.kbytes': 104_857_600,
}


def _msk_oauth_cb(_oauth_config: Any) -> tuple[str, float]:
    """OAUTHBEARER token callback for AWS MSK IAM auth."""
    from aws_msk_iam_sasl_signer import MSKAuthTokenProvider  # noqa: PLC0415

    auth_token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(
        'us-east-1',
    )
    return auth_token, expiry_ms / 1000


class KafkaOutputHandler(OutputHandler):
    """Sends events as JSON to a Kafka topic (requires confluent-kafka)."""

    def __init__(
        self,
        topic: str,
        bootstrap_servers: str = 'localhost:9092',
        producer_config: dict[str, Any] | None = None,
    ) -> None:
        """Initialize Kafka producer."""
        from confluent_kafka import Producer  # noqa: PLC0415

        conf: dict[str, Any] = {
            'bootstrap.servers': bootstrap_servers,
        }
        if producer_config:
            conf.update(producer_config)
        self._topic = topic
        self._producer = Producer(conf)
        # Warm up: establish broker connection before the monitor loop.
        self._producer.produce(self._topic, value=b'')
        self._producer.flush(timeout=30)
        logger.info(
            'KafkaOutputHandler: topic=%s servers=%s (connection ready)',
            topic,
            bootstrap_servers,
        )

    def send(self, payloads: list[dict[str, Any]]) -> None:
        """Produce each payload as a JSON message to Kafka."""
        for payload in payloads:
            self._producer.produce(
                self._topic,
                value=json.dumps(payload).encode('utf-8'),
            )
        self._producer.poll(0)

    def flush(self) -> None:
        """Flush pending Kafka messages."""
        self._producer.flush(timeout=10)

    def close(self) -> None:
        """Flush remaining messages and clean up."""
        self.flush()
        logger.info('KafkaOutputHandler: flushed and closed.')


__all__ = [
    'JsonFileOutputHandler',
    'KafkaOutputHandler',
    'OutputHandler',
    'StdoutOutputHandler',
]
