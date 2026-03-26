from enum import Enum
from typing import Dict

from py4j.java_gateway import JavaObject, get_java_class
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.java_gateway import get_gateway


# modified from flink-python/pyflink/datastream/connectors/kafka.py
# with org.apache.flink.kafka.shaded removed from the jvm path
class KafkaTopicPartition(object):
    """
    Corresponding to Java ``org.apache.kafka.common.TopicPartition`` class.

    Example:
    ::

        >>> topic_partition = KafkaTopicPartition('TOPIC1', 0)

    .. versionadded:: 1.16.0
    """

    def __init__(self, topic: str, partition: int):
        self._topic = topic
        self._partition = partition

    def _to_j_topic_partition(self):
        jvm = get_gateway().jvm
        return jvm.org.apache.kafka.common.TopicPartition(self._topic, self._partition)

    def __eq__(self, other):
        if not isinstance(other, KafkaTopicPartition):
            return False
        return self._topic == other._topic and self._partition == other._partition

    def __hash__(self):
        return 31 * (31 + self._partition) + hash(self._topic)


# with org.apache.flink.kafka.shaded removed from the path
class KafkaOffsetResetStrategy(Enum):
    """
    Corresponding to Java ``org.apache.kafka.client.consumer.OffsetResetStrategy`` class.

    .. versionadded:: 1.16.0
    """

    LATEST = 0
    EARLIEST = 1
    NONE = 2

    def _to_j_offset_reset_strategy(self):
        JOffsetResetStrategy = (
            get_gateway().jvm.org.apache.kafka.clients.consumer.OffsetResetStrategy
        )
        return getattr(JOffsetResetStrategy, self.name)


class KafkaOffsetsInitializer(object):
    """
    An interface for users to specify the starting / stopping offset of a KafkaPartitionSplit.

    .. versionadded:: 1.16.0
    """

    def __init__(self, j_initializer: JavaObject):
        self._j_initializer = j_initializer

    @staticmethod
    def committed_offsets(
        offset_reset_strategy: "KafkaOffsetResetStrategy" = KafkaOffsetResetStrategy.NONE,
    ) -> "KafkaOffsetsInitializer":
        """
        Get an :class:`KafkaOffsetsInitializer` which initializes the offsets to the committed
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
                offset_reset_strategy._to_j_offset_reset_strategy()
            )
        )

    @staticmethod
    def timestamp(timestamp: int) -> "KafkaOffsetsInitializer":
        """
        Get an :class:`KafkaOffsetsInitializer` which initializes the offsets in each partition so
        that the initialized offset is the offset of the first record whose record timestamp is
        greater than or equals the give timestamp.

        :param timestamp: the timestamp to start the consumption.
        :return: an :class:`OffsetsInitializer` which initializes the offsets based on the given
            timestamp.
        """
        JOffsetsInitializer = get_gateway().jvm.org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
        return KafkaOffsetsInitializer(JOffsetsInitializer.timestamp(timestamp))

    @staticmethod
    def earliest() -> "KafkaOffsetsInitializer":
        """
        Get an :class:`KafkaOffsetsInitializer` which initializes the offsets to the earliest
        available offsets of each partition.

        :return: an :class:`KafkaOffsetsInitializer` which initializes the offsets to the earliest
            available offsets.
        """
        JOffsetsInitializer = get_gateway().jvm.org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
        return KafkaOffsetsInitializer(JOffsetsInitializer.earliest())

    @staticmethod
    def latest() -> "KafkaOffsetsInitializer":
        """
        Get an :class:`KafkaOffsetsInitializer` which initializes the offsets to the latest offsets
        of each partition.

        :return: an :class:`KafkaOffsetsInitializer` which initializes the offsets to the latest
            offsets.
        """
        JOffsetsInitializer = get_gateway().jvm.org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
        return KafkaOffsetsInitializer(JOffsetsInitializer.latest())

    @staticmethod
    def offsets(
        offsets: Dict["KafkaTopicPartition", int],
        offset_reset_strategy: "KafkaOffsetResetStrategy" = KafkaOffsetResetStrategy.EARLIEST,
    ) -> "KafkaOffsetsInitializer":
        """
        Get an :class:`KafkaOffsetsInitializer` which initializes the offsets to the specified
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
            None, get_java_class(jvm.Long)
        )
        for tp, offset in offsets.items():
            j_map_wrapper.put(tp._to_j_topic_partition(), offset)

        JOffsetsInitializer = get_gateway().jvm.org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
        return KafkaOffsetsInitializer(
            JOffsetsInitializer.offsets(
                j_map_wrapper.asMap(),
                offset_reset_strategy._to_j_offset_reset_strategy(),
            )
        )


def get_kafka_source(topic: str, partition: int, bound: int):
    builder = (
        KafkaSource.builder()
        .set_bootstrap_servers(
            "b-1-public.diaspora.jdqn8o.c18.kafka.us-east-1.amazonaws.com:9198,"
            "b-2-public.diaspora.jdqn8o.c18.kafka.us-east-1.amazonaws.com:9198"
        )
        .set_group_id("aaa-100")
        .set_property("enable.auto.commit", "false")
        .set_property("security.protocol", "SASL_SSL")
        .set_property("sasl.mechanism", "AWS_MSK_IAM")
        .set_property("fetch.max.bytes", "104857600")  # 100 MB
        .set_property(
            "sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;"
        )
        .set_property(
            "sasl.client.callback.handler.class",
            "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
        )
        .set_value_only_deserializer(SimpleStringSchema())
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
    )

    builder.set_partitions({KafkaTopicPartition(topic, partition)})

    if bound == 0:
        builder.set_bounded(KafkaOffsetsInitializer.latest())
    else:
        builder.set_bounded(
            KafkaOffsetsInitializer.offsets(
                {KafkaTopicPartition(topic, partition): bound}
            )
        )

    return builder.build()


def get_kafka_sink(topic):
    kafka_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(
            "b-1-public.diaspora.jdqn8o.c18.kafka.us-east-1.amazonaws.com:9198,"
            "b-2-public.diaspora.jdqn8o.c18.kafka.us-east-1.amazonaws.com:9198"
        )
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .set_property("security.protocol", "SASL_SSL")
        .set_property("sasl.mechanism", "AWS_MSK_IAM")
        .set_property(
            "sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;"
        )
        .set_property(
            "sasl.client.callback.handler.class",
            "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
        )
        .set_property("linger.ms", "1000")
        .set_property("batch.num.messages", "1000")
        .set_property("batch.size", "100000")
        .set_property("queue.buffering.max.messages", "2147483647")
        .set_property("queue.buffering.max.kbytes", "2147483647")
        .set_property("socket.send.buffer.bytes", f"{512 * 1024}")
        .set_property("compression.type", "lz4")
        .set_property("acks", "1")
        .set_property("max.in.flight.requests.per.connection", "5")
        .set_property("buffer.memory", "67108864")
        .set_property("max.request.size", f"{1048576 * 20}")  # 20 MB
        .set_property("max.block.ms", "90000")
        .set_property("request.timeout.ms", "90000")
        .set_property("delivery.timeout.ms", "120000")
        .set_property("retries", "3")
        .build()
    )
    return kafka_sink
