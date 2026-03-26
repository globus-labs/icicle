# count the size of the docs in topic search-alpha-primary-ingest-results and search-alpha-secondary-ingest-results
from collections import defaultdict
from typing import Dict, Final, Tuple
import json

from auxiliary import get_consumer


def process_message(msg_value) -> list[int]:
    """Return a list of sizes (bytes) for each gmeta element."""
    try:
        data = json.loads(msg_value)
        msg = data["msg"].split()
        cnt, sz = msg[1], msg[3][1:]
        return int(cnt), int(sz)

    except Exception as e:
        print(f"Error processing message: {e}")
        return []


def read_ingest_documents(consumer):
    """Aggregate counts by worker for users, groups, and prefixes."""

    counts, sizes = [], []
    while True:
        msg = consumer.poll(5.0)
        if msg is None:
            break
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        count, size = process_message(msg.value())
        counts.append(count)
        sizes.append(size)

    print(sum(counts), sum(sizes))


request_topic = "search-alpha-primary-ingest-results"
consumer = get_consumer(request_topic)
# read_ingest_documents(consumer)

request_topic = "search-alpha-secondary-ingest-results"
consumer = get_consumer(request_topic)
# read_ingest_documents(consumer)
