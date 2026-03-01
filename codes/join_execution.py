import copy
import numpy as np
import threading
import duckdb
from typing import List
from models import Join
import config
from merge_ops import merge
from utils import get_time


TIMEOUT_S = 905


def execute_join(pos: int, joins: List[Join], size1: float, size2: float):
    n_joins = copy.deepcopy(joins)
    join = joins[pos]
    output_table, new_col_map = merge(join, size1, size2)
    
    for i, j in enumerate(n_joins):
        if i == pos:
            continue
        old_table1 = j.table1
        old_table2 = j.table2
        
        if old_table1 == join.table1 or old_table1 == join.table2:
            j.table1 = output_table
            j.attr1 = [new_col_map[f"{old_table1}.{v}"] for v in j.attr1]
            config.join_id_cnt += 1
            j.join_id = config.join_id_cnt

        if old_table2 == join.table1 or old_table2 == join.table2:
            j.table2 = output_table
            j.attr2 = [new_col_map[f"{old_table2}.{v}"] for v in j.attr2]
            config.join_id_cnt += 1
            j.join_id = config.join_id_cnt

        assert j.table1 != j.table2
        if j.table1 < j.table2:
            j.table1, j.table2 = j.table2, j.table1
            j.attr1, j.attr2 = j.attr2, j.attr1

    vis = np.zeros(len(n_joins), dtype=bool)
    vis[pos] = True

    for i in range(0, len(n_joins)):
        if vis[i]:
            continue
        for k in range(i + 1, len(n_joins)):
            if vis[k]:
                continue
            if (n_joins[i].table1 == n_joins[k].table1 and 
                n_joins[i].table2 == n_joins[k].table2):
                for a in n_joins[k].attr1:
                    n_joins[i].attr1.append(a)
                for a in n_joins[k].attr2:
                    n_joins[i].attr2.append(a)
                vis[k] = True

    new_joins = []
    for i in range(0, len(n_joins)):
        if not vis[i]:
            new_joins.append(n_joins[i])
    return output_table, new_joins


def _exec_query(con, sql: str, done_event, err_box):
    """Execute query in a separate thread."""
    try:
        con.execute(sql)
    except Exception as e:
        err_box.append(e)
    finally:
        done_event.set()


def run_with_timeout(con, sql: str, timeout_s: int = TIMEOUT_S):
    """Run a query with timeout protection."""
    done = threading.Event()
    err_box = []
    t = threading.Thread(target=_exec_query, args=(con, sql, done, err_box), 
                        daemon=True)
    t.start()

    if not done.wait(timeout_s):
        con.interrupt()
        done.wait(5)
        raise TimeoutError(f"Query exceeded {timeout_s}s and was interrupted")

    if err_box:
        raise err_box[0]


def exe_final_join(con):
    """Execute final join queries with retry logic."""
    final_count = 0
    if config.run_umbra_flag:
        print(config.umbra_sql_file_path)
        sql_file = open(config.umbra_sql_file_path, "w")
        sql_file.write(f"""
\o -
\set timeout 5400000
\echo running {config.query_name} {config.db_name}
""")
        config.umbra_sql += "\n\echo join time:\n"
        for sql in config.final_joins:
            config.umbra_sql += sql + ";"
        # print(config.umbra_sql)
        # exit(0)
        sql_file.write(config.umbra_sql)
        config.umbra_sql = ""
        sql_file.close()
        return
    if len(config.final_joins) != 1:
        con.execute("SET disabled_optimizers = 'join_order,build_side_probe_side';")
    
    union_sql = ""
    finished = True
    for i, sql in enumerate(config.final_joins):
        if i == 0:
            union_sql = sql
        else:
            union_sql += '\nUNION ALL\n' + sql
    
    min_time = None
    for i in range(4):
        dt = None
        try:
            run_with_timeout(con, union_sql)
            dt = get_time(con, "join")
        except TimeoutError:
            print(f"[TIMEOUT] {config.db_name} - repetition {i+1} exceeded "
                  f"{TIMEOUT_S}s and was interrupted.")
            continue
        except duckdb.Error as e:
            msg = str(e).lower()
            if "out of memory" in msg or "could not allocate" in msg or "oom" in msg:
                print(f"[OOM] {config.db_name} - repetition {i+1} aborted "
                      f"due to out-of-memory.")
                continue
            elif "interrupted" in msg:
                print(f"[TIMEOUT] {config.db_name} - repetition {i+1} exceeded "
                      f"{TIMEOUT_S}s and was interrupted.")
                continue
            else:
                raise
        min_time = dt if (min_time is None) else min(min_time, dt)
    
    if min_time is not None:
        config.total_time = min_time
        with open(f"./duckdb_split_profile/{config.query_name}/{config.db_name}.log", 
                 "w") as f:
            for i, sql in enumerate(config.final_joins):
                if i == 0:
                    union_sql = sql
                else:
                    union_sql += '\nUNION ALL\n' + sql
                res = con.execute(f"EXPLAIN ANALYZE {sql}").fetchall()
                for row in res:
                    for col in row:
                        f.write(str(col) + "\n")
    else:
        finished = False
        print("[RESULT] No successful repetition to report (all timed out / OOM).")
    
    con.execute("SET disabled_optimizers = '';")
    if finished:
        print(f"join_time: {min_time:.3f}")
