"""A module for output handlers."""

from __future__ import annotations

import json
import logging
from abc import ABC
from abc import abstractmethod
from contextlib import ExitStack
from pathlib import Path
from typing import Any
from typing import TextIO

from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from confluent_kafka import Producer

from monitor.conf import MSKOptions
from monitor.conf import OutputDestination
from monitor.conf import OutputOptions

logger = logging.getLogger(__name__)


class OutputHandler(ABC):
    """Base class for output handlers."""

    @abstractmethod
    def send(self, payloads: list[dict[str, Any]]) -> None:
        """Send a batch of payloads to the output destination."""
        pass

    @abstractmethod
    def close(self) -> None:
        """Close the output handler."""
        pass


class NoneOutputHandler(OutputHandler):
    """A handler that ignores all events."""

    class _NullProducer:
        """Placeholder producer exposing a flush no-op."""

        def flush(self) -> None:
            """Simulate Producer.flush() without side effects."""

    def __init__(self) -> None:
        """Create the handler with a no-op producer stub."""
        self.producer = self._NullProducer()

    def send(self, payloads: list[dict[str, Any]]) -> None:
        """Intentionally do nothing when receiving payload batches."""

    def close(self) -> None:
        """No resources to release."""


def oauth_cb(oauth_config: Any) -> tuple[str, float]:
    """Callback for OAUTHBEARER token generation for MSK IAM."""
    auth_token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(
        'us-east-1',
    )
    return auth_token, expiry_ms / 1000


def get_producer_conf(msk_options: MSKOptions) -> dict[str, Any]:
    """Return Kafka producer configuration for AWS MSK with OAUTHBEARER."""
    conf = msk_options.config_dict.copy()
    conf['oauth_cb'] = oauth_cb
    return conf


class KafkaOutputHandler(OutputHandler):
    """Handles sending events to a Kafka topic."""

    def __init__(self, topic: str | None, msk_options: MSKOptions):
        """Initialize the Kafka output handler."""
        if not topic:
            raise ValueError('Kafka topic cannot be empty.')
        self.topic = topic
        try:
            self.producer = Producer(get_producer_conf(msk_options))
        except Exception as e:
            logger.error(f'Failed to initialize Kafka Producer: {e}')
            raise
        logger.debug(
            f"KafkaOutputHandler initialized for topic '{self.topic}'",
        )

    def send(self, payloads: list[dict[str, Any]]) -> None:
        """Send payloads to the Kafka topic."""
        try:
            for payload in payloads:
                self.producer.produce(
                    topic=self.topic,
                    value=json.dumps(payload).encode('utf-8'),
                )
            self.producer.poll(0)
        except Exception as e:
            logger.error(f'Failed to produce message to Kafka: {e}')
            raise

    def close(self) -> None:
        """Flush and close the Kafka producer."""
        try:
            logger.debug('Flushing Kafka producer...')
            self.producer.flush()
            logger.debug('Kafka producer flushed.')
        except Exception as e:
            logger.error(f'Error while flushing Kafka producer: {e}')
            raise


class JsonFileOutputHandler(OutputHandler):
    """Writes events as a single JSON array to a file."""

    def __init__(self, path: str):
        """Initialize the file output handler."""
        self.path = Path(path)
        self._exit_stack = ExitStack()
        self._f: TextIO = self._exit_stack.enter_context(
            self.path.open('w', encoding='utf-8'),
        )
        self._is_first_item = True
        self._f.write('[\n')

    def send(self, payloads: list[dict[str, Any]]) -> None:
        """Write payloads to the JSON array."""
        for payload in payloads:
            if not self._is_first_item:
                self._f.write(',\n')
            json.dump(payload, self._f)
            self._is_first_item = False
        if payloads:
            self._f.flush()

    def close(self) -> None:
        """Close the JSON array and file."""
        if self._f:
            self._f.write('\n]\n')
        self._exit_stack.close()


def get_output_handler(
    options: OutputOptions,
) -> OutputHandler:
    """Return a handler instance for the configured destination."""
    if options.destination is OutputDestination.KAFKA:
        handle = options.handle
        if not handle:
            raise ValueError('Kafka destination requires a topic name.')
        return KafkaOutputHandler(handle, options.msk)

    if options.destination is OutputDestination.JSON:
        handle = options.handle
        if not handle:
            raise ValueError('JSON destination requires a file name.')
        return JsonFileOutputHandler(handle)

    return NoneOutputHandler()
