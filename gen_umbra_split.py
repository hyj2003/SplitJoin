import duckdb
import pandas as pd
import numpy as np
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
from itertools import count
from typing import List, Tuple, Dict, Set
import time
import copy
import re
import math
import json
import os

sql_file = None
drop_list: List[str] = []
column_dict: Dict[str, Set[str]] = {}
def get_column_names(table_name):
    return list(column_dict[table_name])

@dataclass
class Join:
    table1: str
    table2: str
    attr1: List[str]
    attr2: List[str]
    label: int
    join_id: int
    factor: float
    factor_L: float
join_id_cnt: int = 0
        
def read_joins() -> List[Join]:
    n = int(input())
    relations = []
    for i in range(n):
        line = input().strip()
        parts = line.split()
        if len(parts) != 4:
            print("Error: Each line must have 4 parts")
            continue
        table1, table2, attr1, attr2 = parts
        global join_id_cnt
        join_id_cnt += 1
        relations.append(Join(table1, table2, [attr1], [attr2], 0, join_id_cnt, 0, 0))
    return relations

cache_thres: Dict[str, int] = {}
cache_split: Dict[str, int] = {}
original_table = set()
intermediate_cnt = count()
thres_dict: Dict[Tuple[str, str], Tuple[int, str]] = {}
sample_dict: Dict[str, str] = {}
variable_dict: Dict[str, Set[int]] = {}
basetable_dict: Dict[str, Set[str]] = {}
triangles: Set[frozenset[int]] = set()
table_size: Dict[str, int] = {}
attr_variable: Dict[str, int] = {}
enable_filter: bool = False
table_sql: Dict[str, str] = {}
table_coe: Dict[str, float] = {}
cache_factor = {}

def get_threshold(con, s_tbl) -> Tuple[int, str]:
    df = con.execute(f"SELECT * FROM {s_tbl}").fetchdf()
    cnts = df['cnt'].values
    l, r, threshold = 0, len(cnts) - 1, 0
    while l <= r:
        mid = (l + r) // 2
        if mid >= cnts[mid]:
            threshold, r = cnts[mid], mid - 1
        else:
            l = mid + 1
    return threshold

# If joins[pos] is heavy, find out which joins will be light
def update_HL(pos: int, joins: List[Join], light: np.ndarray):
    assert (not joins[pos].table1.startswith("intermediate")) and (not joins[pos].table2.startswith("intermediate"))
    assert len(joins[pos].attr1) == 1 and len(joins[pos].attr2) == 1
    t1, t2 = joins[pos].table1, joins[pos].table2
    a1, a2 = joins[pos].attr1[0], joins[pos].attr2[0]
    res = np.array(light)
    for i in range(len(joins)):
        if i == pos:
            continue
        if len(basetable_dict[joins[i].table1]) > 1 or len(basetable_dict[joins[i].table2]) > 1:
            continue
        for a in joins[i].attr1:
            if (a != a1 and joins[i].table1 == t1) or (a != a2 and joins[i].table1 == t2):
                res[i] = 1
                break
        for a in joins[i].attr2:
            if (a != a1 and joins[i].table2 == t1) or (a != a2 and joins[i].table2 == t2):
                res[i] = 1
                break
    return res

def label_joins(joins: List[Join]) -> None:
    cnt = 0
    fa = np.zeros(len(joins) + 1, dtype=np.int32)
    for i in range(len(joins) + 1):
        fa[i] = i
    def getf(x) -> np.int32:
        while x != fa[x]:
            fa[x] = fa[fa[x]]
            x = fa[x]
        return x
    for i in range(0, len(joins)):
        for j in range(0, i): 
            if {f"{joins[i].table1}.{joins[i].attr1[0]}", f"{joins[i].table2}.{joins[i].attr2[0]}"} & \
               {f"{joins[j].table1}.{joins[j].attr1[0]}", f"{joins[j].table2}.{joins[j].attr2[0]}"}:
                fa[getf(i + 1)] = getf(j + 1)

    for i in range(0, len(joins)):
        if joins[getf(i + 1) - 1].label == 0:
            cnt += 1
            joins[getf(i + 1) - 1].label = cnt
        joins[i].label = joins[getf(i + 1) - 1].label
db_size = 0
def split_join(con, join: Join, split_table: str, thres: int):
    global intermediate_cnt
    assert len(join.attr1) == 1 and len(join.attr2) == 1
    # print(f"Split table: {join.table1} {join.table2}")
    tables = [join.table1, join.table2]
    attrs = [join.attr1[0], join.attr2[0]]
    new_tables = []

    for i in range(len(attrs)):
        for condition in ["<=", ">"]:
            current_value = next(intermediate_cnt)
            output_table = f"intermediate_{current_value}"
            # print(f"output_table: {output_table}, split_table: {split_table}")
            if condition == "<=":
                table_sql[output_table] = f"""
(SELECT t.* 
FROM {db_name} t
LEFT JOIN {split_table} c ON t.{attrs[i]} = c.val
WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END {condition} {thres}) AS {output_table}
"""
                table_coe[output_table] = 0.99 * db_size
            else:                
                table_sql[output_table] = f"""
(SELECT t.* 
FROM {db_name} t
JOIN (
    SELECT s.*
    FROM {split_table} s
    WHERE s.cnt {condition} {thres}) c
    ON t.{attrs[i]} = c.val) AS {output_table}
"""
                table_coe[output_table] = 0.01 * db_size
            basetable_dict[output_table] = {tables[i]}
            table_size[output_table] = 1
            column_dict[output_table] = copy.deepcopy(column_dict[tables[i]])
            cols = get_column_names(output_table)
            for col in cols:
                # print(f"{output_table}.{col}", f"{tables[i]}.{col}")
                attr_variable[f"{output_table}.{col}"] = attr_variable[f"{tables[i]}.{col}"]
            variable_dict[output_table] = variable_dict[f"{tables[i]}"]
            new_tables.append(output_table)
    return new_tables

final_count = 0
final_joins: List[Join] = []

def merge(join: Join, size1: float, size2: float) -> Tuple[str, Dict[str, str]]:
    global intermediate_cnt
    output_table = f"intermediate_{next(intermediate_cnt)}"
    cols1 = get_column_names(join.table1)
    cols2 = get_column_names(join.table2)
    rename_map = {}

    select_list = []
    
    column_dict[output_table] = set()
    for col in cols1:
        new_name = f"{join.table1}_{col}"
        select_list.append(f"{join.table1}.{col} AS {new_name}")
        rename_map[f"{join.table1}.{col}"] = new_name
        attr_variable[f"{output_table}.{new_name}"] = attr_variable[f"{join.table1}.{col}"]
        column_dict[output_table].add(new_name)
    for col in cols2:
        new_name = f"{join.table2}_{col}"
        select_list.append(f"{join.table2}.{col} AS {new_name}")
        rename_map[f"{join.table2}.{col}"] = new_name
        attr_variable[f"{output_table}.{new_name}"] = attr_variable[f"{join.table2}.{col}"]
        column_dict[output_table].add(new_name)
    
    select_clause = ",\n       ".join(select_list) 
    join_conditions = [
        f"{join.table1}.{a1} = {join.table2}.{a2}"
        for a1, a2 in zip(join.attr1, join.attr2)
    ]
    join_clause = " AND ".join(join_conditions)
    variable_dict[output_table] = variable_dict[join.table1] | variable_dict[join.table2]
    if size1 < size2:
        join.table1, join.table2 = join.table2, join.table1
        join.attr1, join.attr2 = join.attr2, join.attr1
    table_sql[output_table] = f"""
(SELECT {select_clause}
FROM {table_sql[join.table2]} JOIN {table_sql[join.table1]} ON forceorder({join_clause})) AS {output_table}"""
    return output_table, rename_map

def execute_join(pos: int, joins: List[Join], size1: float, size2: float):
    n_joins = copy.deepcopy(joins)
    join = joins[pos]
    output_table, new_col_map = merge(join, size1, size2)
    for i, j in enumerate(n_joins):
        if i == pos:
            continue
        old_table1 = j.table1
        old_table2 = j.table2
        global join_id_cnt
        if old_table1 == join.table1 or old_table1 == join.table2:
            j.table1 = output_table
            j.attr1 = [new_col_map[f"{old_table1}.{v}"] for v in j.attr1]
            join_id_cnt += 1
            j.join_id = join_id_cnt

        if old_table2 == join.table1 or old_table2 == join.table2:
            j.table2 = output_table
            j.attr2 = [new_col_map[f"{old_table2}.{v}"] for v in j.attr2]
            join_id_cnt += 1
            j.join_id = join_id_cnt

        assert j.table1 != j.table2
        if j.table1 < j.table2:
            j.table1, j.table2 = j.table2, j.table1
            j.attr1, j.attr2 = j.attr2, j.attr1

    vis = np.zeros(len(n_joins), dtype=bool)
    vis[pos] = True

    for i in range(0, len(n_joins)):
        if vis[i]: continue
        for k in range(i + 1, len(n_joins)):
            if vis[k]: continue
            if n_joins[i].table1 == n_joins[k].table1 and n_joins[i].table2 == n_joins[k].table2:
                for a in n_joins[k].attr1:
                    n_joins[i].attr1.append(a)
                for a in n_joins[k].attr2:
                    n_joins[i].attr2.append(a)
                vis[k] = True

    new_joins = []
    for i in range(0, len(n_joins)):
        if vis[i]:
            continue
        new_joins.append(n_joins[i])
    return output_table, new_joins


def exe_final_join(con):
    final_count = 0
    union_sql = ""
    for sql in final_joins:
        if union_sql == "":
            union_sql = sql + ";"
        else:
            union_sql += sql + ";"
    sql_file.write(union_sql)

def join_phase(con, joins: List[Join], light: np.ndarray):
    table_list = []
    for j in joins:
        if j.table1 not in table_list:
            table_list.append(j.table1)
        if j.table2 not in table_list:
            table_list.append(j.table2)
        # print(basetable_dict[j.table1], basetable_dict[j.table2])
    n = len(table_list)
    # print("n:", n)
    # for t in table_list:
    #     print("base_tables, table_coe:", basetable_dict[t], table_coe[t])
    # print(light)
    # f[s]: min cost
    # g[s]: current size
    # h[s]: f[s] transferred from which subset
    f, g, h = np.full(1 << n, -1, dtype=float), np.full(1 << n, -1, dtype=float), np.zeros(1 << n, dtype=int)
    for i in range(n):
        f[1 << i], g[1 << i] = 0, table_coe[table_list[i]]
    joins_copy = copy.deepcopy(joins)
    for i, j in enumerate(joins):
        if light[i] == 2:
            # j.factor /= (table_coe[j.table1] / db_size) * (table_coe[j.table2] / db_size)
            j.factor = 1
        # print(basetable_dict[j.table1], basetable_dict[j.table2], table_coe[j.table1], table_coe[j.table2], j.factor, j.factor_L, light[i])
    def compute_size(s, t):
        var_s, var_t = set(), set()
        for i in range(n):
            if s >> i & 1 != 0:
                var_s = var_s | variable_dict[table_list[i]]
            if t >> i & 1 != 0:
                var_t = var_t | variable_dict[table_list[i]]
        res_f, res_g = min(g[s], g[t]) * 1.0 + max(g[s], g[t]) * 0.1 + f[s] + f[t], 0
        if var_s == var_t:
            res_g = min(g[s], g[t])
        elif var_s <= var_t:
            res_g = g[t]
        elif var_s >= var_t:
            res_g = g[s]
        else:
            res_g = 1
            for i, j in enumerate(joins):
                idx1, idx2 = table_list.index(j.table1), table_list.index(j.table2)
                in_s1, in_s2 = (s >> idx1) & 1, (s >> idx2) & 1
                in_t1, in_t2 = (t >> idx1) & 1, (t >> idx2) & 1
                if (in_s1 and in_t2) or (in_s2 and in_t1):
                    if light[i] == 1:
                        res_g = min(res_g, j.factor_L)
                    else:
                        res_g = min(res_g, j.factor)
            res_g = res_g * g[s] * g[t]
        return res_f, res_g
    for s in range(1, 1 << n):
        t = s
        while t:
            t = (t - 1) & s
            if t == 0:
                break
            cur_f, cur_g = compute_size(s ^ t, t)
            # print("s, cur_f, cur_g:", s, cur_f, cur_g)
            g[s] = cur_g if g[s] < 0 else min(g[s], cur_g)
            if f[s] < 0:
                f[s], h[s] = cur_f, t
            else:
                if cur_f < f[s]:
                    f[s], h[s] = cur_f, t
    def print_plan(s: int, indent: int = 0) -> str:
        prefix = "  " * indent
        if h[s] == 0:
            idx = int(np.log2(s))
            return f"{prefix}Table: {basetable_dict[table_list[idx]]}"
        t = h[s]
        ls = print_plan(s ^ t, indent + 2)
        rs = print_plan(t, indent + 2)
        if g[s ^ t] > g[t]:
            ls, rs = rs, ls
            t = s ^ t
        left_size = g[s ^ t]
        right_size = g[t]
        result = f"{prefix}JOIN\n"
        result += f"{'  ' * (indent + 1)}Left (size={left_size}):\n{ls}\n"
        result += f"{'  ' * (indent + 1)}Right (size={right_size}):\n{rs}"
        return result
    def gen_plan(s: int, indent: int = 0): # build side is left
        prefix = "  " * indent
        if h[s] == 0:
            idx = int(np.log2(s))
            return table_list[idx], f"{prefix}Table: {basetable_dict[table_list[idx]]}"
        t = h[s]
        l_table, l_plan = gen_plan(s ^ t, indent + 2)
        r_table, r_plan = gen_plan(t, indent + 2)
        pos = 0
        nonlocal joins
        for i, j in enumerate(joins):
            if (j.table1 == l_table and j.table2 == r_table):
                pos = i
                break
            if (j.table2 == l_table and j.table1 == r_table):
                pos = i
                j.table1, j.table2 = j.table2, j.table1
                j.attr1, j.attr2 = j.attr2, j.attr1
                break
        vl, vr = g[s ^ t], g[t]
        sl, sr = bin(s ^ t).count('1'),  bin(t).count('1')
        if sl != sr:
            if sl == 3 and len(variable_dict[l_table]) == 3:
                sl = 1.5
            if sr == 3 and len(variable_dict[r_table]) == 3:
                sr = 1.5
            vl, vr = sl, sr
        output_table, joins = execute_join(pos, joins, vl, vr)
        plan = f"{prefix}JOIN\n"
        if vl < vr:
            plan += f"{'  ' * (indent + 1)}Left:\n{r_plan}\n"
            plan += f"{'  ' * (indent + 1)}Right:\n{l_plan}"
        else:
            plan += f"{'  ' * (indent + 1)}Left:\n{l_plan}\n"
            plan += f"{'  ' * (indent + 1)}Right:\n{r_plan}"
        return output_table, plan

    final_table, plan = gen_plan((1 << n) - 1)
    final_joins.append(f"""
SELECT COUNT(*)
FROM {table_sql[final_table]}
""")
    joins = joins_copy
    return

min_j_list, min_max, min_cnt = [], 10000000, 10000000
def pick_split(joins: List[Join], j_list: List[Tuple[int, str, str]], vis: Set[str], cnt: int):
    cand_joins = []
    for i, j in enumerate(joins):
        if j.table1 in vis or j.table2 in vis:
            continue
        for t in triangles:
            if j.join_id in t:
                cand_joins.append((i, j.table1, j.table2))
    if len(cand_joins) == 0:
        for i, j in enumerate(joins):
            if j.table1 in vis or j.table2 in vis:
                continue
            cand_joins.append((i, j.table1, j.table2))
    if len(cand_joins) == 0:
        global min_j_list, min_max, min_cnt
        s = 0
        for j in j_list:
            s = max(s, thres_dict[(joins[j[0]].table1, joins[j[0]].table2)][0])
        if s < min_max:
            min_j_list, min_max, min_cnt = j_list, s, cnt
        elif s == min_max and min_cnt > cnt:
            min_j_list, min_max, min_cnt = j_list, s, cnt
        return
    for j in cand_joins:
        pick_split(joins, j_list + [j], vis | {j[1], j[2]}, cnt + 1)

def create_j_list():
    global min_j_list, min_max, min_cnt
    min_j_list, min_max, min_cnt = [], 10000000, 10000000
    pick_split(joins, [], set(), 0)
    tmp_j_list = []
    for j in min_j_list:
        k, d = cache_factor[thres_dict[(j[1], j[2])][1]]
        if k > 240 or k * 5 < d:
            tmp_j_list.append(j)
    min_j_list = tmp_j_list

def split_phase(con, joins: List[Join], light: np.ndarray, j_list: List[Tuple[int, str, str]]):
    if len(j_list) == 0:
        n_light, n_joins = copy.deepcopy(light), copy.deepcopy(joins)
        join_phase(con, n_joins, n_light)
        return
    j0 = j_list[0]
    p = j0[0]
    thres, s_tbl = thres_dict[(j0[1], j0[2])]
    new_tables = split_join(con, joins[p], s_tbl, thres)
    org_tables = [joins[p].table1, joins[p].table2]
    # For joins[i] is heavy
    n_light, n_joins = update_HL(p, joins, light), copy.deepcopy(joins)
    n_light[p] = 2
    for join in n_joins:
        for i, tbl in enumerate(org_tables):
            if join.table1 == tbl:
                join.table1 = new_tables[2 * i + 1]
            if join.table2 == tbl:
                join.table2 = new_tables[2 * i + 1]
    split_phase(con, n_joins, n_light, j_list[1:])
    # For joins[i] is light
    n_light, n_joins = copy.deepcopy(light), copy.deepcopy(joins)
    n_light[p] = 1
    for join in n_joins:
        for i, tbl in enumerate(org_tables):
            if join.table1 == tbl:
                join.table1 = new_tables[2 * i]
            if join.table2 == tbl:
                join.table2 = new_tables[2 * i]
    split_phase(con, n_joins, n_light, j_list[1:])

def clear():
    global final_count, final_joins
    final_count = 0
    final_joins = []
    global original_table, intermediate_cnt, thres_dict, sample_dict,\
          variable_dict, basetable_dict, triangles, table_size,\
            attr_variable, enable_filter, join_id_cnt, table_sql, tags_dict
    original_table = set()
    intermediate_cnt = count(0)
    thres_dict = {}
    sample_dict = {}
    variable_dict = {}
    basetable_dict = {}
    triangles = set()
    table_size = {}
    attr_variable = {}
    enable_filter = False
    join_id_cnt = 0
    table_sql = {}
    tags_dict = {}
    global column_dict
    column_dict = {}
    global time_info
    time_info = {}
    global cache_split, cache_thres, table_coe
    cache_split, cache_thres, table_coe = {}, {}, {}
query_name = ""
def run_once(joins, db_name):
    global sql_file
    print(f"Query Path: ./umbra/{query_name}/split_{db_name}.sql")
    sql_file = open(f"./umbra/{query_name}/split_{db_name}.sql", "w")
    sql_file.write(f"""
\o -
\set timeout 5400000
\echo running {query_name} {db_name}
""")
    for j in joins:
        if j.table1 not in variable_dict:
            variable_dict[j.table1] = set()
            column_dict[j.table1] = set()
        variable_dict[j.table1].add(j.label)
        column_dict[j.table1].add(j.attr1[0])
        attr_variable[f"{j.table1}.{j.attr1[0]}"] = j.label

        if j.table2 not in variable_dict:
            variable_dict[j.table2] = set()
            column_dict[j.table2] = set()
        variable_dict[j.table2].add(j.label)
        column_dict[j.table2].add(j.attr2[0])
        attr_variable[f"{j.table2}.{j.attr2[0]}"] = j.label

        original_table.add(j.table1)
        original_table.add(j.table2)
        print(j.label, j.table1, j.table2)
    # enable_filter = len(original_table) != 3
    # print("\n")
    for i in range(len(joins)):
        for j in range(i+1, len(joins)):
            if not ({joins[i].table1, joins[i].table2} &\
               {joins[j].table1, joins[j].table2}) or joins[i].label == joins[j].label:
                continue
            for k in range(j+1, len(joins)):
                if (not ({joins[i].table1, joins[i].table2} &\
               {joins[k].table1, joins[k].table2})) or \
                (not ({joins[j].table1, joins[j].table2} &\
                {joins[k].table1, joins[k].table2})):
                    continue
                s = frozenset([joins[i].table1, joins[i].table2, \
                        joins[j].table1, joins[j].table2, \
                        joins[k].table1, joins[k].table2, ])
                if len({joins[i].label, joins[j].label, joins[k].label}) == 3 and len(s) == 3:
                    triangles.add(frozenset([joins[i].join_id, joins[j].join_id, joins[k].join_id]))

    con = duckdb.connect(database=':memory:conn')
    con.execute("SET memory_limit = '200GB';")
    print(f"Database {db_name} loading.")
    con.execute(f"IMPORT DATABASE '{db_name}'")

    for t in original_table:
        basetable_dict[t] = {t}
        table_size[t] = 1
        table_sql[t] = f"{db_name} AS {t}"
    print(f"Database {db_name} loaded.")
    con.execute(f"CREATE TABLE group_obj AS (SELECT obj AS val, COUNT(*) AS cnt FROM {db_name} GROUP BY obj ORDER BY cnt DESC LIMIT 100000);")
    con.execute(f"CREATE TABLE group_sub AS (SELECT sub AS val, COUNT(*) AS cnt FROM {db_name} GROUP BY sub ORDER BY cnt DESC LIMIT 100000);")
    con.execute(f"""
CREATE TABLE sub_obj AS (
SELECT group_obj.val AS val, LEAST(group_obj.cnt, group_sub.cnt) AS cnt
FROM group_obj JOIN group_sub
ON group_obj.val = group_sub.val
ORDER BY cnt DESC);
    """)
    sql_file.write(f"""
DROP TABLE IF EXISTS group_obj, group_sub, sub_obj;
CREATE TABLE group_obj AS (SELECT obj AS val, COUNT(*) AS cnt FROM {db_name} GROUP BY obj ORDER BY cnt DESC LIMIT 100000);
CREATE TABLE group_sub AS (SELECT sub AS val, COUNT(*) AS cnt FROM {db_name} GROUP BY sub ORDER BY cnt DESC LIMIT 100000);
CREATE TABLE sub_obj AS (
SELECT group_obj.val AS val, LEAST(group_obj.cnt, group_sub.cnt) AS cnt
FROM group_obj JOIN group_sub
ON group_obj.val = group_sub.val
ORDER BY cnt DESC);
\echo join time:
\set repeat 4
""")
    global db_size
    db_size = con.execute(f"SELECT COUNT(*) FROM {db_name};").fetchone()[0]
    for t in original_table:
        table_coe[t] = db_size
    global cache_thres, cache_factor
    cache_factor = {}
    cache_thres = {}
    for s_tbl in ["sub_obj", "group_sub", "group_obj"]:
        cache_thres[s_tbl] = get_threshold(con, s_tbl)
        max_degree = con.execute(f"SELECT MAX(cnt) FROM {s_tbl}").fetchone()[0]
        cache_factor[s_tbl] = (cache_thres[s_tbl], max_degree)
        
    thres_sub_obj = cache_thres["sub_obj"]
    global s_tbl_table, s_tbl_attr
    s_tbl_table, s_tbl_attr = ("group_sub", "sub") if cache_thres["group_sub"] < cache_thres["group_obj"] else ("group_obj", "obj")
    for i, join in enumerate(joins):
        s_tbl = ""
        if join.attr1[0] != join.attr2[0]:
            s_tbl = "sub_obj"
        elif join.attr1[0] == 'sub':
            s_tbl = "group_sub"
        else: 
            s_tbl = "group_obj"
        j_size_L, j_size = cache_factor[s_tbl]
        join.factor, join.factor_L = j_size / db_size, j_size_L / db_size
        thres_dict[(join.table1, join.table2)] = (j_size_L, s_tbl)
        thres_dict[(join.table2, join.table1)] = (j_size_L, s_tbl)
    create_j_list()
    global min_j_list
    print("min_j_list:", min_j_list)
    if len(min_j_list) == 0:
        sql = "SELECT COUNT(*) FROM "
        for i, t in enumerate(sorted(original_table)):
            if i == 0: sql += f"{db_name} AS {t}"
            else: sql += f", {db_name} AS {t}"
        sql += "\nWHERE "
        for i, j in enumerate(joins):
            if i == 0: sql += f"{j.table1}.{j.attr1[0]} = {j.table2}.{j.attr2[0]}"
            else: sql += f" AND {j.table1}.{j.attr1[0]} = {j.table2}.{j.attr2[0]}"
        sql += ";"
        sql_file.write(sql)
    else:
        split_phase(con, copy.deepcopy(joins), np.zeros(len(joins), dtype=int), min_j_list)
    exe_final_join(con)
    sql_file.close()
    con.close()
    clear()

if __name__ == "__main__":
    query_name = input()
    joins = read_joins()
    label_joins(joins)
    tmp_joins = copy.deepcopy(joins)
    os.sched_setaffinity(0, set(range(0, 31)))
    db_names = ['wgpb', 'orkut', 'gplus', 'uspatent', 'skitter', 'topcats']
    for db_name in db_names:
        print(f"running {query_name} {db_name}")
        print("len(joins)", len(joins))
        joins = copy.deepcopy(tmp_joins)
        run_once(joins, db_name)