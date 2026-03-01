import re
from typing import Set, List
import config


def extract_total_time(text: str) -> float:
    """Extract total execution time from profiling output."""
    match = re.search(r"Total Time:\s*([\d.]+)s", text)
    if match:
        return float(match.group(1))
    else:
        raise ValueError("Total Time not found")


def timed_execute_sql(con, sql: str, op: str = "other"):
    """Execute SQL and return the result."""
    res = con.execute(sql)
    return res


def get_time(con, op: str = "other") -> float:
    """Get execution time from profiling and update time_info."""
    con.execute(f"""
        PRAGMA disable_profile;
        PRAGMA enable_profile;
    """)
    with open(config.PROFILING_PATH, "r") as f:
        text = f.read()
    res = extract_total_time(text)
    if op in config.time_info:
        config.time_info[op] += res
    else:
        config.time_info[op] = res
    return res


def get_column_names(table_name: str) -> List[str]:
    """Get column names for a given table."""
    return list(config.column_dict[table_name])
