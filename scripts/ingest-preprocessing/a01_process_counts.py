from __future__ import annotations

from collections import defaultdict
from typing import Final

from auxiliary import get_consumer


def process_message(msg_value) -> tuple:
    """Process a single message and return its components."""
    try:
        typ, rest = msg_value.decode('utf-8').strip().split(',', 1)
        name, worker, count = rest.rsplit(',', 2)
        return typ, name, int(worker), int(count)

    except Exception:
        print(msg_value)


def aggregate_counts_by_worker(consumer):
    """Aggregate counts by worker for users, groups, and prefixes."""
    users: dict[tuple[int, int], int] = {}
    groups: dict[tuple[int, int], int] = {}
    prefixes: dict[tuple[str, int], int] = defaultdict(int)

    while True:
        msg = consumer.poll(5.0)
        if msg is None:
            break
        if msg.error():
            print(f'Consumer error: {msg.error()}')
            continue

        typ, name, worker, count = process_message(msg.value())

        if typ == 'user':
            users[(int(name), worker)] = count
        elif typ == 'group':
            groups[(int(name), worker)] = count
        else:
            # should not do // here, in conflict with expand_prefixes
            # must do after expand_prefixes
            prefixes[(name, worker)] = count

    return users, groups, prefixes


def aggregate_counts_total(
    users: dict[tuple[int, int], int],
    groups: dict[tuple[int, int], int],
    prefixes: dict[tuple[str, int], int],
) -> tuple[dict[int, int], dict[int, int], dict[str, int]]:
    """Aggregate counts across all workers for verification."""
    total_users: dict[int, int] = defaultdict(int)
    total_groups: dict[int, int] = defaultdict(int)
    total_prefixes: dict[str, int] = defaultdict(int)

    for (name, _), count in users.items():
        total_users[name] += count

    for (name, _), count in groups.items():
        total_groups[name] += count

    for (prefix, _), count in prefixes.items():
        total_prefixes[prefix] += count

    return total_users, total_groups, total_prefixes


def write_worker_csv(
    users: dict[tuple[int, int], int],
    groups: dict[tuple[int, int], int],
    prefixes: dict[tuple[str, int], int],
    output_file: str,
) -> None:
    """Write the worker-specific counts to a CSV file."""
    with open(output_file, mode='w', encoding='utf-8') as file:
        for (name, worker), count in sorted(users.items()):
            file.write(f'user, {name}, {worker}, {count}\n')

        for (name, worker), count in sorted(groups.items()):
            file.write(f'group, {name}, {worker}, {count}\n')

        for (prefix, worker), count in sorted(
            prefixes.items(),
            key=lambda x: (x[0][0].count('/'), x[0], x[1]),
        ):
            file.write(f'prefix, {prefix}, {worker}, {count}\n')


def write_verification_csv(
    total_users: dict[int, int],
    total_groups: dict[int, int],
    total_prefixes: dict[str, int],
    output_file: str,
) -> None:
    """Write the total aggregated counts to a verification CSV file."""
    with open(output_file, mode='w', encoding='utf-8') as file:
        for name, count in sorted(total_users.items()):
            file.write(f'user, {name}, {count}\n')

        for name, count in sorted(total_groups.items()):
            file.write(f'group, {name}, {count}\n')

        for prefix, count in sorted(
            total_prefixes.items(),
            key=lambda x: (x[0].count('/'), x[0], x[1]),
        ):
            file.write(f'prefix, {prefix}, {count}\n')


def get_prefix(filename: str, level: int) -> str:
    """Return the directory prefix consisting of the first `level` path components.
    - If level <= 0: return "/".
    - If the file's directory depth < level: return "" (no prefix).
    Assumes `filename` is an absolute file path (e.g., "/a/b/c.txt") and does not end with "/".
    """
    ROOT: Final[str] = '/'
    if level <= 0:
        return ROOT

    # Split into components; last part is the file name
    parts = filename.strip('/').split('/')
    dir_parts = parts[:-1]  # exclude the file name
    depth = len(dir_parts)

    if depth < level:
        return ''  # not enough depth to produce the requested prefix

    # Join the first `level` directories
    return ROOT + '/'.join(dir_parts[:level]) if level > 0 else ROOT


def expand_prefixes(prefixes, prefix_min, prefix_max):
    levels = [defaultdict(lambda: 0) for _ in range(prefix_min, prefix_max)]

    for (name, worker), count in prefixes.items():
        if len(name) == 1:
            lvl = 0
        else:
            lvl = name.count('/')
        idx = lvl - prefix_min
        levels[idx][(name, worker)] = count

    print([len(e) for e in levels])

    for i in range(
        len(levels) - 2,
        -1,
        -1,
    ):  # for the second to the last level
        nxt_lvl_dict = levels[i + 1]
        cur_lvl = i + prefix_min
        for (name, worker), count in nxt_lvl_dict.items():
            name_prefix = get_prefix(name, cur_lvl)
            levels[i][(name_prefix, worker)] += count

    print([len(e) for e in levels])

    aggregate = {}
    for lvl_dict in levels:
        for k, v in lvl_dict.items():
            aggregate[k] = v

    return aggregate


def shrink_workers(prefixes):
    new_prefixes = {}

    for (name, worker), count in prefixes.items():
        if name.count('/') == 2:
            worker = worker // 2
        elif name.count('/') == 3:
            worker = worker // 4
        elif name.count('/') >= 4:
            worker = worker // 8

        new_prefixes[(name, worker)] = (
            new_prefixes.get((name, worker), 0) + count
        )
    return new_prefixes


def consume_to_csv_with_worker(
    request_topic: str,
    result_csv: str,
    verify_csv: str,
) -> None:
    """Process messages from Kafka topic and write both worker-specific and total counts to CSV files."""
    consumer = get_consumer(request_topic)

    # Get counts by worker
    users, groups, prefixes = aggregate_counts_by_worker(consumer)

    if 'hpss' in result_csv:
        pmin, pmax = 1, 5
    elif 'nersc' in result_csv:
        pmin, pmax = 0, 5
    elif 'itap' in result_csv:
        pmin, pmax = 1, 5
    else:
        raise
    prefixes = expand_prefixes(prefixes, pmin, pmax)
    prefixes = shrink_workers(prefixes)
    # Print statistics
    print(
        f'# of users-worker: {len(users)}, total files: {sum(users.values())}',
    )
    print(
        f'# of groups-worker: {len(groups)}, total files: {sum(groups.values())}',
    )
    # print(f"# of prefix-worker: {len(prefixes)}, total files: {sum(prefixes.values())}")

    # Get total counts across all workers
    total_users, total_groups, total_prefixes = aggregate_counts_total(
        users,
        groups,
        prefixes,
    )

    # Write both CSV files
    write_worker_csv(users, groups, prefixes, result_csv)
    write_verification_csv(
        total_users,
        total_groups,
        total_prefixes,
        verify_csv,
    )


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description='Consume counting results from Kafka and write CSVs.',
    )
    parser.add_argument(
        '--request-topic',
        default='search-alpha-counts',
        help='Kafka topic to consume (default: search-alpha-counts)',
    )
    parser.add_argument(
        '--result-csv',
        required=True,
        help='Output CSV with per-worker counts (e.g. itap.agg2.csv)',
    )
    parser.add_argument(
        '--verify-csv',
        required=True,
        help='Output CSV with total counts (e.g. itap.veri.csv)',
    )
    args = parser.parse_args()
    consume_to_csv_with_worker(
        args.request_topic,
        args.result_csv,
        args.verify_csv,
    )
