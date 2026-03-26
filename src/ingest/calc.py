"""Backward-compatible re-exports from decomposed modules.

All public names that were previously in calc.py are re-exported
here so that existing imports (e.g., ``from calc import get_prefix``)
continue to work.
"""

from __future__ import annotations

from aggregation import get_usr_grp_dir_dicts
from aggregation import GetPrefixWorkerCols
from aggregation import SecondaryMap
from aggregation import SecondaryReduce
from batch import BatchGmeta
from batch import create_search_client
from batch import exception_to_json
from conf import CMode
from conf import IngestConfig
from pipeline import counting_pipeline
from pipeline import get_watermark_strategy
from pipeline import MyTimestampAssigner
from pipeline import primary_pipeline
from pipeline import read_from_s3
from pipeline import save_counts_to_octopus
from pipeline import secondary_pipeline
from prefix import get_prefix
from sketches import calc_file_stats
from sketches import calc_time_stats

# Keep old names as aliases for backward compatibility.
calc_file_stats_dd = calc_file_stats
calc_file_stats_apache = calc_file_stats
calc_time_stats_dd = calc_time_stats
calc_time_stats_apache = calc_time_stats

__all__ = [
    'BatchGmeta',
    'CMode',
    'GetPrefixWorkerCols',
    'IngestConfig',
    'MyTimestampAssigner',
    'SecondaryMap',
    'SecondaryReduce',
    'calc_file_stats',
    'calc_file_stats_apache',
    'calc_file_stats_dd',
    'calc_time_stats',
    'calc_time_stats_apache',
    'calc_time_stats_dd',
    'counting_pipeline',
    'create_search_client',
    'exception_to_json',
    'get_prefix',
    'get_usr_grp_dir_dicts',
    'get_watermark_strategy',
    'primary_pipeline',
    'read_from_s3',
    'save_counts_to_octopus',
    'secondary_pipeline',
]
