"""CSV schema definitions for each HPC dataset."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class CsvSchema:
    """Column layout for a specific HPC dataset."""

    n_cols: int
    path_col: int
    user_col: int = 2
    group_col: int = 3
    size_col: int = 4
    atime_col: int = 5
    ctime_col: int = 6
    mtime_col: int = 7
    type_col: int = 0
    mode_col: int = 1
    extra_content: Callable[[list[str]], dict[str, Any]] | None = None


ITAP_SCHEMA = CsvSchema(n_cols=9, path_col=8)  # fs-small
HPSS_SCHEMA = CsvSchema(n_cols=9, path_col=8)  # fs-large
NERSC_SCHEMA = CsvSchema(
    n_cols=10,
    path_col=9,
    extra_content=lambda cols: {'fileset_name': cols[8]},
)  # fs-medium
