"""Directory prefix extraction utilities."""

from __future__ import annotations


def get_prefix(filename: str, level: int) -> str:
    """Return the directory prefix of the first `level` path components.

    - If level <= 0: return "/".
    - If the file's directory depth < level: return "" (no prefix).

    Assumes `filename` is an absolute file path (e.g., "/a/b/c.txt")
    and does not end with "/".
    """
    root = '/'
    if level <= 0:
        return root

    parts = filename.strip('/').split('/')
    dir_parts = parts[:-1]
    depth = len(dir_parts)

    if depth < level:
        return ''

    return root + '/'.join(dir_parts[:level])
