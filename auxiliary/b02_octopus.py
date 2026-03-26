import csv
import json
import os
from typing import Dict, Iterable, List, Tuple

from auxiliary import get_consumer


MetricRow = Dict[str, int | str]


METRICS: List[Tuple[str, Dict[str, str]]] = [
    (
        "size",
        {
            "min": "file_size_min_in_bytes",
            "p10": "file_size_10p_in_bytes",
            "p25": "file_size_25p_in_bytes",
            "p50": "file_size_median_in_bytes",
            "p75": "file_size_75p_in_bytes",
            "p90": "file_size_90p_in_bytes",
            "p99": "file_size_99p_in_bytes",
            "max": "file_size_max_in_bytes",
            "mean": "file_size_average_in_bytes",
        },
    ),
    (
        "atime",
        {
            "min": "access_date_min",
            "p10": "access_date_10p",
            "p25": "access_date_25p",
            "p50": "access_date_median",
            "p75": "access_date_75p",
            "p90": "access_date_90p",
            "p99": "access_date_99p",
            "max": "access_date_max",
            "mean": "access_date_average",
        },
    ),
    (
        "ctime",
        {
            "min": "change_date_min",
            "p10": "change_date_10p",
            "p25": "change_date_25p",
            "p50": "change_date_median",
            "p75": "change_date_75p",
            "p90": "change_date_90p",
            "p99": "change_date_99p",
            "max": "change_date_max",
            "mean": "change_date_average",
        },
    ),
    (
        "mtime",
        {
            "min": "mod_date_min",
            "p10": "mod_date_10p",
            "p25": "mod_date_25p",
            "p50": "mod_date_median",
            "p75": "mod_date_75p",
            "p90": "mod_date_90p",
            "p99": "mod_date_99p",
            "max": "mod_date_max",
            "mean": "mod_date_average",
        },
    ),
]


def process_message(msg_value: bytes) -> List[Tuple[str, str, dict]]:
    msg = json.loads(msg_value)
    assert msg["mock"]
    assert msg["succeed"]

    gmeta_list = msg["data"]["ingest_data"]["gmeta"]
    out: List[Tuple[str, str, dict]] = []
    for gmeta in gmeta_list:
        typ, name = gmeta["subject"].split("::")
        raw = gmeta["content"]["_raw"]
        out.append((typ, name, raw))
    return out


def aggregate_stats(consumer) -> Tuple[Dict[int, dict], Dict[int, dict]]:
    """Aggregate ddsketch stats for users and groups."""
    users: Dict[int, dict] = {}
    groups: Dict[int, dict] = {}

    while True:
        msg = consumer.poll(5.0)
        if msg is None:
            break
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue
        for typ, name, raw in process_message(msg.value()):
            if typ == "user_id":
                users[int(name)] = raw
            elif typ == "group_id":
                groups[int(name)] = raw

    return users, groups


def iter_metric_rows(entity_id: int, raw: dict, id_key: str) -> Iterable[MetricRow]:
    """Yield CSV-ready rows for each metric present in a raw record."""
    for metric_name, fields in sorted(METRICS, key=lambda item: item[0]):
        row: MetricRow = {
            id_key: entity_id,
            "metric": metric_name,
            "count": raw.get("file_count", ""),
        }
        for csv_key, raw_key in fields.items():
            row[csv_key] = raw.get(raw_key, "")
        yield row


def write_metric_csv(records: Dict[int, dict], output_file: str, id_key: str) -> None:
    fieldnames = [
        id_key,
        "metric",
        "count",
        "min",
        "p10",
        "p25",
        "p50",
        "p75",
        "p90",
        "p99",
        "max",
        "mean",
    ]
    os.makedirs(os.path.dirname(output_file) or ".", exist_ok=True)
    with open(output_file, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for entity_id in sorted(records):
            for row in iter_metric_rows(entity_id, records[entity_id], id_key):
                writer.writerow(row)


def consume_to_csv(topic: str, uid_csv: str, gid_csv: str) -> None:
    """Consume ddsketch summaries and write user/group CSVs."""
    consumer = get_consumer(topic)
    users, groups = aggregate_stats(consumer)

    print(f"# of users: {len(users)}")
    print(f"# of groups: {len(groups)}")

    write_metric_csv(users, uid_csv, "uid")
    write_metric_csv(groups, gid_csv, "gid")
    print(f"wrote {os.path.abspath(uid_csv)}")
    print(f"wrote {os.path.abspath(gid_csv)}")


if __name__ == "__main__":
    folder = "hpss-approx"  # itap-approx
    sketch = "td"  # dd, kll, req, td
    topic = "search-alpha-secondary-ingest-results"

    for idx in [1, 2, 3]:

        if idx > 1:
            topic += f"-{int(idx)}"
        uid_csv = f"{folder}/uid-{sketch}-{idx}.csv"
        gid_csv = f"{folder}/gid-{sketch}-{idx}.csv"
        for path in (uid_csv, gid_csv):
            if os.path.exists(path):
                print(f"{os.path.abspath(path)} exists, not overwriting.")
                raise SystemExit(1)

        consume_to_csv(topic, uid_csv, gid_csv)
