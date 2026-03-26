"""Kafka source and sink builders for AWS MSK with IAM auth."""

from __future__ import annotations

import os
from enum import Enum
from typing import Any

from py4j.java_gateway import get_java_class
from py4j.java_gateway import JavaObject
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import KafkaRecordSerializationSchema
from pyflink.datastream.connectors.kafka import KafkaSink
from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.java_gateway import get_gateway

_DEFAULT_BROKERS = (
    'b-1-public.diaspora.jdqn8o.c18.kafka.us-east-1.amazonaws.com:9198,'
    'b-2-public.diaspora.jdqn8o.c18.kafka.us-east-1.amazonaws.com:9198'
)

_KAFKA_SECURITY_PROPS: dict[str, str] = {
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'AWS_MSK_IAM',
    'sasl.jaas.config': (
        'software.amazon.msk.auth.iam.IAMLoginModule required;'
    ),
    'sasl.client.callback.handler.class': (
        'software.amazon.msk.auth.iam.IAMClientCallbackHandler'
    ),
}


def _get_brokers() -> str:
    return os.environ.get('KAFKA_BROKERS', _DEFAULT_BROKERS)


def _apply_security_props(builder: Any) -> Any:
    for k, v in _KAFKA_SECURITY_PROPS.items():
        builder.set_property(k, v)
    return builder


# The KafkaTopicPartition, KafkaOffsetResetStrategy, and KafkaOffsetsInitializer
# classes below are modified from Apache Flink:
#   flink-python/pyflink/datastream/connectors/kafka.py
# with org.apache.flink.kafka.shaded removed from the jvm path.
#
# Original source: Copyright 2014-2025 The Apache Software Foundation
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
class KafkaTopicPartition:
    """Corresponding to Java ``org.apache.kafka.common.TopicPartition`` class.

    Example:
    ::

        >>> topic_partition = KafkaTopicPartition('TOPIC1', 0)

    .. versionadded:: 1.16.0
    """

    def __init__(self, topic: str, partition: int) -> None:
        self._topic = topic
        self._partition = partition

    def _to_j_topic_partition(self) -> Any:
        jvm = get_gateway().jvm
        return jvm.org.apache.kafka.common.TopicPartition(
            self._topic,
            self._partition,
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, KafkaTopicPartition):
            return False
        return (
            self._topic == other._topic and self._partition == other._partition
        )

    def __hash__(self) -> int:
        return 31 * (31 + self._partition) + hash(self._topic)


class KafkaOffsetResetStrategy(Enum):
    """Corresponding to Java ``org.apache.kafka.client.consumer.OffsetResetStrategy`` class.

    .. versionadded:: 1.16.0
    """

    LATEST = 0
    EARLIEST = 1
    NONE = 2

    def _to_j_offset_reset_strategy(self) -> Any:
        JOffsetResetStrategy = get_gateway().jvm.org.apache.kafka.clients.consumer.OffsetResetStrategy
        return getattr(JOffsetResetStrategy, self.name)


class KafkaOffsetsInitializer:
    """An interface for users to specify the starting / stopping offset of a KafkaPartitionSplit.

    .. versionadded:: 1.16.0
    """

    def __init__(self, j_initializer: JavaObject):
        self._j_initializer = j_initializer

    @staticmethod
    def committed_offsets(
        offset_reset_strategy: KafkaOffsetResetStrategy = KafkaOffsetResetStrategy.NONE,
    ) -> KafkaOffsetsInitializer:
        """Get an :class:`KafkaOffsetsInitializer` which initializes the offsets to the committed
        offsets. An exception will be thrown at runtime if there is no committed offsets.

        An optional :class:`KafkaOffsetResetStrategy` can be specified to initialize the offsets if
        the committed offsets does not exist.

        :param offset_reset_strategy: the offset reset strategy to use when the committed offsets do
            not exist.
        :return: an offset initializer which initialize the offsets to the committed offsets.
        """
        JOffsetsInitializer = get_gateway().jvm.org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
        return KafkaOffsetsInitializer(
            JOffsetsInitializer.committedOffsets(
                offset_reset_strategy._to_j_offset_reset_strategy(),
            ),
        )

    @staticmethod
    def timestamp(timestamp: int) -> KafkaOffsetsInitializer:
        """Get an :class:`KafkaOffsetsInitializer` which initializes the offsets in each partition so
        that the initialized offset is the offset of the first record whose record timestamp is
        greater than or equals the give timestamp.

        :param timestamp: the timestamp to start the consumption.
        :return: an :class:`OffsetsInitializer` which initializes the offsets based on the given
            timestamp.
        """
        JOffsetsInitializer = get_gateway().jvm.org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
        return KafkaOffsetsInitializer(
            JOffsetsInitializer.timestamp(timestamp),
        )

    @staticmethod
    def earliest() -> KafkaOffsetsInitializer:
        """Get an :class:`KafkaOffsetsInitializer` which initializes the offsets to the earliest
        available offsets of each partition.

        :return: an :class:`KafkaOffsetsInitializer` which initializes the offsets to the earliest
            available offsets.
        """
        JOffsetsInitializer = get_gateway().jvm.org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
        return KafkaOffsetsInitializer(JOffsetsInitializer.earliest())

    @staticmethod
    def latest() -> KafkaOffsetsInitializer:
        """Get an :class:`KafkaOffsetsInitializer` which initializes the offsets to the latest offsets
        of each partition.

        :return: an :class:`KafkaOffsetsInitializer` which initializes the offsets to the latest
            offsets.
        """
        JOffsetsInitializer = get_gateway().jvm.org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
        return KafkaOffsetsInitializer(JOffsetsInitializer.latest())

    @staticmethod
    def offsets(
        offsets: dict[KafkaTopicPartition, int],
        offset_reset_strategy: KafkaOffsetResetStrategy = KafkaOffsetResetStrategy.EARLIEST,
    ) -> KafkaOffsetsInitializer:
        """Get an :class:`KafkaOffsetsInitializer` which initializes the offsets to the specified
        offsets.

        An optional :class:`KafkaOffsetResetStrategy` can be specified to initialize the offsets in
        case the specified offset is out of range.

        Example:
        ::

            >>> KafkaOffsetsInitializer.offsets({
            ...     KafkaTopicPartition('TOPIC1', 0): 0,
            ...     KafkaTopicPartition('TOPIC1', 1): 10000
            ... }, KafkaOffsetResetStrategy.EARLIEST)

        :param offsets: the specified offsets for each partition.
        :param offset_reset_strategy: the :class:`KafkaOffsetResetStrategy` to use when the
            specified offset is out of range.
        :return: an :class:`KafkaOffsetsInitializer` which initializes the offsets to the specified
            offsets.
        """
        jvm = get_gateway().jvm
        j_map_wrapper = jvm.org.apache.flink.python.util.HashMapWrapper(
            None,
            get_java_class(jvm.Long),
        )
        for tp, offset in offsets.items():
            j_map_wrapper.put(tp._to_j_topic_partition(), offset)

        JOffsetsInitializer = get_gateway().jvm.org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
        return KafkaOffsetsInitializer(
            JOffsetsInitializer.offsets(
                j_map_wrapper.asMap(),
                offset_reset_strategy._to_j_offset_reset_strategy(),
            ),
        )


def get_kafka_source(
    topic: str,
    partition: int,
    bound: int,
) -> KafkaSource:
    """Build a Kafka source for a single partition."""
    builder = (
        KafkaSource.builder()
        .set_bootstrap_servers(_get_brokers())
        .set_group_id('aaa-100')
        .set_property('enable.auto.commit', 'false')
        .set_property('fetch.max.bytes', '104857600')
        .set_value_only_deserializer(SimpleStringSchema())
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
    )
    _apply_security_props(builder)

    builder.set_partitions({KafkaTopicPartition(topic, partition)})

    if bound == 0:
        builder.set_bounded(KafkaOffsetsInitializer.latest())
    else:
        builder.set_bounded(
            KafkaOffsetsInitializer.offsets(
                {KafkaTopicPartition(topic, partition): bound},
            ),
        )

    return builder.build()


def get_kafka_sink(topic: str) -> KafkaSink:
    """Build a Kafka sink for a topic with AT_LEAST_ONCE delivery."""
    builder = (
        KafkaSink.builder()
        .set_bootstrap_servers(_get_brokers())
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build(),
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
    )
    _apply_security_props(builder)
    kafka_sink = (
        builder.set_property('linger.ms', '1000')
        .set_property('batch.num.messages', '1000')
        .set_property('batch.size', '100000')
        .set_property('queue.buffering.max.messages', '2147483647')
        .set_property('queue.buffering.max.kbytes', '2147483647')
        .set_property('socket.send.buffer.bytes', f'{512 * 1024}')
        .set_property('compression.type', 'lz4')
        .set_property('acks', '1')
        .set_property('max.in.flight.requests.per.connection', '5')
        .set_property('buffer.memory', '67108864')
        .set_property('max.request.size', f'{1048576 * 20}')  # 20 MB
        .set_property('max.block.ms', '90000')
        .set_property('request.timeout.ms', '90000')
        .set_property('delivery.timeout.ms', '120000')
        .set_property('retries', '3')
        .build()
    )
    return kafka_sink
