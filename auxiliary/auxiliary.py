from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from confluent_kafka import Consumer, TopicPartition


def oauth_cb(oauth_config):
    auth_token, expiry_ms = MSKAuthTokenProvider.generate_auth_token("us-east-1")
    # print(auth_token)
    return auth_token, expiry_ms / 1000


def get_producer_conf():
    return {
        "bootstrap.servers": "b-2-public.diaspora.jdqn8o.c18.kafka.us-east-1.amazonaws.com:9198,b-1-public.diaspora.jdqn8o.c18.kafka.us-east-1.amazonaws.com:9198",
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "OAUTHBEARER",
        "oauth_cb": oauth_cb,
        "linger.ms": 10,
        "batch.num.messages": 1000,
        "compression.type": "gzip",
        "acks": 0,
    }


def get_consumer_conf():
    return {
        # "debug": "all",
        "bootstrap.servers": "b-2-public.diaspora.jdqn8o.c18.kafka.us-east-1.amazonaws.com:9198,b-1-public.diaspora.jdqn8o.c18.kafka.us-east-1.amazonaws.com:9198",
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "OAUTHBEARER",
        "oauth_cb": oauth_cb,
        "group.id": "bbb-100",
        "enable.auto.commit": "false",
        "auto.offset.reset": "earliest",
        "fetch.min.bytes": 1048576,  # 1MB
        "fetch.max.bytes": 10485760,  # 10MB
        "max.partition.fetch.bytes": 10485760,  # 10MB per partition
        "fetch.wait.max.ms": 50,
        "allow.auto.create.topics": "false",
        "session.timeout.ms": 45000,  # Increased to avoid frequent rebalances
    }


def get_consumer(request_topic):
    consumer = Consumer(get_consumer_conf())
    topic_partition = TopicPartition(request_topic, partition=0)
    consumer.assign([topic_partition])
    return consumer
