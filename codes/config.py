"""Configuration module for DuckDB query processing."""

import os
from typing import Dict, Set, Tuple
from itertools import count

# Profiling settings
PROFILING_PATH = "/users/heyujun/SplitJoin/duckdb_split_profile/profile1.log"

# Global dictionaries and sets
time_info: Dict[str, float] = {}
column_dict: Dict[str, Set[str]] = {}
cache_thres: Dict[str, int] = {}
original_table: Set[str] = set()
thres_dict: Dict[Tuple[str, str], Tuple[int, str]] = {}
sample_dict: Dict[str, str] = {}
variable_dict: Dict[str, Set[int]] = {}
basetable_dict: Dict[str, Set[str]] = {}
triangles: Set[frozenset[int]] = set()
table_size: Dict[str, int] = {}
attr_variable: Dict[str, int] = {}
table_sql: Dict[str, str] = {}
table_coe: Dict[str, float] = {}
cache_factor: Dict[str, Tuple[int, int]] = {}
umbra_sql: str = ""
run_umbra_flag: bool = False
umbra_sql_file_path: str = ""
umbra_precheck_sql: str = ""
filtered_table: str = ""
filtered_table_sampled: str = ""

# Use materialized ctes to compute thresholds.
compute_thresholds_sql: str = ""

# Global counters
intermediate_cnt = count()
join_id_cnt: int = 0

# Settings
enable_filter: bool = False
ratio: int = 100  # sampling ratio = {ratio} percent
db_name: str = ''
db_size: int = 0
label_range: int = 3

# Final results
final_count: int = 0
final_joins = []
total_time: float = 0

# Split strategy
min_j_list = []
min_max: int = 10000000
min_cnt: int = 10000000
query_name = ""

def reset_globals():
    """Reset all global variables to their initial state."""
    global final_count, final_joins, original_table, intermediate_cnt
    global thres_dict, sample_dict, variable_dict, basetable_dict
    global triangles, table_size, attr_variable, enable_filter
    global join_id_cnt, table_sql, column_dict, time_info, cache_thres
    global min_j_list, min_max, min_cnt, total_time, compute_thresholds_sql, umbra_sql
    total_time = 0
    final_count = 0
    final_joins = []
    original_table = set()
    intermediate_cnt = count(0)
    thres_dict = {}
    sample_dict = {}
    variable_dict = {}
    basetable_dict = {}
    triangles = set()
    table_size = {}
    attr_variable = {}
    join_id_cnt = 0
    table_sql = {}
    column_dict = {}
    time_info = {}
    cache_thres = {}
    min_j_list = []
    min_max = 10000000
    min_cnt = 10000000
    compute_thresholds_sql = ""
    umbra_sql = ""
