import duckdb
import pandas as pd
import numpy as np
import threading
import time
import math
import json
import os
import re
import psutil

profiling_path = "duckdb_baseline_profile/profile.log"
time_info = {}
def extract_total_time(text: str) -> float:
    match = re.search(r"Total Time:\s*([\d.]+)s", text)
    if match:
        return float(match.group(1))
    else:
        raise ValueError("Total Time not found")
def get_time(con, op: str="other"):
    con.execute(f"""
        PRAGMA disable_profile;
        PRAGMA enable_profile;
    """)
    with open(profiling_path, "r") as f:
        text = f.read()
    res = extract_total_time(text)
    return res

TIMEOUT_S = 905

def _exec_query(con, sql, done_event, err_box):
    try:
        result = con.execute(sql).fetchall()
        # for i in range(len(result)):
        #     for j in range(len(result[i])):
        #         print(result[i][j])
    except Exception as e:
        err_box.append(e)
    finally:
        done_event.set()

def run_with_timeout(con, sql, timeout_s=TIMEOUT_S):
    done = threading.Event()
    err_box = []
    t = threading.Thread(target=_exec_query, args=(con, sql, done, err_box), daemon=True)
    t.start()

    if not done.wait(timeout_s):
        con.interrupt()
        done.wait(5)
        raise TimeoutError(f"Query exceeded {timeout_s}s and was interrupted")

    if err_box:
        raise err_box[0]

def run_once(sql, db_name, timeout_s=TIMEOUT_S):
    con = duckdb.connect(database=':memory:conn')
    con.execute("SET threads = 32;")
    con.execute("SET memory_limit = '220GB';")
    con.execute("PRAGMA temp_directory=''")
    print(f"Database {db_name} loading.")
    con.execute(f"IMPORT DATABASE '{db_name}'")
    print(f"Database {db_name} loaded.")
    con.execute("PRAGMA enable_profile")
    con.execute("SET enable_profiling = 'query_tree'")
    global profiling_path
    profiling_path = f"duckdb_baseline_profile/{query_name}_{db_name}.log"
    con.execute(f"""
        SET profiling_output = '{profiling_path}';
        SET profile_output   = '{profiling_path}';
    """)
    try:
        min_time = None
        for i in range(4):
            dt = None
            try:
                run_with_timeout(con, sql, timeout_s=timeout_s)
                dt = get_time(con, "join")
            except TimeoutError:
                print(f"[TIMEOUT] {db_name} - repetition {i+1} exceeded {timeout_s}s and was interrupted.")
                continue
            except duckdb.Error as e:
                msg = str(e).lower()
                if "out of memory" in msg or "could not allocate" in msg or "oom" in msg:
                    print(f"[OOM] {db_name} - repetition {i+1} aborted due to out-of-memory.")
                    continue
                elif "interrupted" in msg:
                    print(f"[TIMEOUT] {db_name} - repetition {i+1} exceeded {timeout_s}s and was interrupted.")
                    continue
                else:
                    raise
            min_time = dt if (min_time is None) else min(min_time, dt)
        if min_time is not None:
            print(f"total_time: {min_time:.3f}s")
        else:
            print("[RESULT] No successful repetition to report (all timed out / OOM).")

    finally:
        con.close()

if __name__ == "__main__":
    query_names = ['q1', 'q2', 'q3', 'q4', 'q5', 'q6', 'q7', 'q8', 'q9', 'q10', 'q11', ]
    db_names = ['wgpb', 'orkut', 'gplus', 'uspatent', 'skitter', 'topcats']
    os.sched_setaffinity(0, set(range(32)))
    for i, query_name in enumerate(query_names):
        for db_name in db_names:
            print(f"running {query_name} {db_name}")
            with open(f"./query_duckdb/{query_name}.sql", "r") as f:
                sql = f.read()
            sql = sql.replace("{db_name}", db_name)
            run_once(sql, db_name)