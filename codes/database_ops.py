import copy
import time
import duckdb
import numpy as np
from typing import List
from models import Join
import config
from threshold import get_threshold
from join_labeling import find_triangles
from split_strategy import create_j_list, split_phase
from join_execution import exe_final_join


def initialize_database(con, db_name: str):
    """Initialize database connection and load data."""
    con.execute("SET threads = 32;")
    con.execute("SET memory_limit = '220GB';")
    con.execute("PRAGMA temp_directory=''")
    print(f"Database {db_name} loading.")
    con.execute(f"IMPORT DATABASE '{db_name}'")
    print(f"Database {db_name} loaded.")


def setup_profiling(con):
    """Setup query profiling."""
    con.execute("PRAGMA enable_profile")
    con.execute("SET enable_profiling = 'query_tree'")
    con.execute(f"""
    SET profiling_output = '{config.PROFILING_PATH}';
    SET profile_output = '{config.PROFILING_PATH}';
    """)

def precheck_thresholds(con, joins, db_name: str):
    """Pre-check if a query is already light if filter exists."""
    start = time.time()
    ctes = []
    selects = []
    precheck_sample_ratio = 5
    config.compute_thresholds_sql = "WITH "
    for i in range(config.label_range):
        ctes.append(f"""
sampled_tbl_{i} AS MATERIALIZED (
SELECT sub, obj
FROM {db_name}
WHERE label = {i} AND hash(rowid) % 100 < {precheck_sample_ratio})
,
group_obj_{i} AS MATERIALIZED (
    SELECT obj AS val, CAST(COUNT(*) * {100 / precheck_sample_ratio} AS BIGINT) AS cnt 
    FROM sampled_tbl_{i}
    GROUP BY obj 
    ORDER BY cnt DESC 
    LIMIT 5000)
,
group_sub_{i} AS MATERIALIZED (
    SELECT sub AS val, CAST(COUNT(*) * {100 / precheck_sample_ratio} AS BIGINT) AS cnt 
    FROM sampled_tbl_{i}
    GROUP BY sub 
    ORDER BY cnt DESC 
    LIMIT 5000)
""")
    for i, join in enumerate(joins):
        s_tbl = f"sub_obj_{i}"
        t0, t1 = int(join.table1[1:]) % config.label_range, int(join.table2[1:]) % config.label_range
        a0, a1 = join.attr1[0], join.attr2[0]
        ctes.append(f"""
sub_obj_{i} AS MATERIALIZED (
    SELECT t1.val AS val, LEAST(t1.cnt, t2.cnt) AS cnt
    FROM group_{a0}_{t0} AS t1 JOIN group_{a1}_{t1} AS t2
    ON t1.val = t2.val
    ORDER BY cnt DESC
)
""")
        # if i != len(joins):
        #     config.compute_thresholds_sql += ","
    for i, join in enumerate(joins):
        s_tbl = f"sub_obj_{i}"
        t0, t1 = int(join.table1[1:]) % config.label_range, int(join.table2[1:]) % config.label_range
        a0, a1 = join.attr1[0], join.attr2[0]
        ctes.append(f"""
thres_{i} AS MATERIALIZED (
    SELECT 
        (SELECT MAX(cnt) FROM {s_tbl}) as max,
        COALESCE(
            (SELECT MAX(cnt)
             FROM (
                 SELECT cnt, row_number() OVER (ORDER BY cnt DESC) - 1 AS idx
                 FROM {s_tbl}
             )
             WHERE cnt <= idx),
            0
        ) as threshold
)
""")
        selects.append(f"SELECT '{s_tbl}' as table_name, threshold, max FROM thres_{i}")
    query = f"""
WITH {', '.join(ctes)}
{' UNION ALL '.join(selects)};
"""
    if config.run_umbra_flag:
        config.umbra_precheck_sql = ""
        # config.umbra_precheck_sql = query.replace("MATERIALIZED", "")
        # config.umbra_precheck_sql = query.replace("MATERIALIZED", "")
    rows = con.execute(query).fetchall()
    # con.execute(query)
    # print(df['threshold'], df['max'])
    print(f"Split check time: {time.time()-start:.3f}")
    # exit(0)
    for table_name, thres, mx in rows:
        thres = 0 if thres is None else thres
        mx = 0 if mx is None else mx
        print(f"thres, mx: {thres}, {mx}")
        if thres <= 100 and mx <= 500:
            return False

    return True

def create_sampling_tables(con, db_name: str):
    """Create sampled tables for heavy hitter detection."""
    start_time = time.time()
    create_table_sql = f"""
CREATE TEMP TABLE sampled_tbl AS (
SELECT sub, obj
FROM {db_name}
WHERE hash(rowid) % 100 < {config.ratio});

CREATE TEMP TABLE group_obj AS (
    SELECT obj AS val, CAST(COUNT(*) * {100 / config.ratio} AS BIGINT) AS cnt 
    FROM sampled_tbl 
    GROUP BY obj 
    ORDER BY cnt DESC 
    LIMIT 10000
);

CREATE TEMP TABLE group_sub AS (
    SELECT sub AS val, CAST(COUNT(*) * {100 / config.ratio} AS BIGINT) AS cnt 
    FROM sampled_tbl 
    GROUP BY sub 
    ORDER BY cnt DESC 
    LIMIT 10000
);

CREATE TEMP TABLE sub_obj AS (
    SELECT group_obj.val AS val, LEAST(group_obj.cnt, group_sub.cnt) AS cnt
    FROM group_obj JOIN group_sub
    ON group_obj.val = group_sub.val
    ORDER BY cnt DESC
);
"""
    con.execute(create_table_sql)
    if config.enable_filter:
        create_table_sqls = f"""
CREATE TEMP TABLE group_obj_f AS (
    SELECT obj AS val, CAST(COUNT(*) * {100 / config.ratio} AS BIGINT) AS cnt  
    FROM {config.filtered_table_sampled}
    GROUP BY obj 
    ORDER BY cnt DESC 
    LIMIT 5000
);

CREATE TEMP TABLE group_sub_f AS (
    SELECT sub AS val, CAST(COUNT(*) * {100 / config.ratio} AS BIGINT) AS cnt 
    FROM {config.filtered_table_sampled} 
    GROUP BY sub 
    ORDER BY cnt DESC 
    LIMIT 5000
);
"""
        con.execute(create_table_sqls)
    end_time = time.time()
    print(f"agg time: {end_time - start_time:.3f} seconds")

def create_sampling_tables_umbra(con, db_name: str):
    """Create sampled tables for heavy hitter detection."""
#     config.umbra_sql += f"""
# \echo warmup begin:
# SELECT 
#     COUNT(*) as total_rows,
#     COUNT(DISTINCT sub) as unique_subs,
#     COUNT(DISTINCT obj) as unique_objs
# FROM {db_name};
# \echo warmup end.
# \echo preprocessing:
# """
    # if not config.enable_filter:
    create_table_sql = f"""
CREATE TEMP TABLE group_obj AS (
    SELECT obj AS val, COUNT(*) AS cnt 
    FROM {db_name} 
    GROUP BY obj 
    ORDER BY cnt DESC 
    LIMIT 10000
);

CREATE TEMP TABLE group_sub AS (
    SELECT sub AS val, COUNT(*) AS cnt 
    FROM {db_name} 
    GROUP BY sub 
    ORDER BY cnt DESC 
    LIMIT 10000
);

CREATE TEMP TABLE sub_obj AS (
    SELECT group_obj.val AS val, LEAST(group_obj.cnt, group_sub.cnt) AS cnt
    FROM group_obj JOIN group_sub
    ON group_obj.val = group_sub.val
    ORDER BY cnt DESC
);
"""
    config.umbra_sql += create_table_sql
    if config.enable_filter:
        create_table_sqls = ""
#         for i in range(config.label_range):
#             create_table_sqls += f"""
# CREATE TEMP TABLE group_obj_{i} AS (
#     SELECT obj AS val, COUNT(*) AS cnt 
#     FROM (SELECT sub, obj
#         FROM {db_name}
#         WHERE label = {i}) t
#     GROUP BY obj 
#     ORDER BY cnt DESC 
#     LIMIT 5000
# );

# CREATE TEMP TABLE group_sub_{i} AS (
#     SELECT sub AS val, COUNT(*) AS cnt 
#     FROM (SELECT sub, obj
#         FROM {db_name}
#         WHERE label = {i}) t
#     GROUP BY sub 
#     ORDER BY cnt DESC 
#     LIMIT 5000
# );
# """
        create_table_sqls = f"""
CREATE TEMP TABLE group_obj_f AS (
    SELECT obj AS val, COUNT(*) AS cnt 
    FROM {config.filtered_table} 
    GROUP BY obj 
    ORDER BY cnt DESC 
    LIMIT 5000
);

CREATE TEMP TABLE group_sub_f AS (
    SELECT sub AS val, COUNT(*) AS cnt 
    FROM {config.filtered_table} 
    GROUP BY sub 
    ORDER BY cnt DESC 
    LIMIT 5000
);
"""
        config.umbra_sql += create_table_sqls

def compute_thresholds(con):
    """Compute thresholds for heavy hitter detection."""
    config.cache_factor = {}
    config.cache_thres = {}
    start_time = time.time()
    for s_tbl in ["sub_obj", "group_sub", "group_obj"]:
        config.cache_thres[s_tbl] = get_threshold(con, s_tbl)
        max_degree = con.execute(f"SELECT MAX(cnt) FROM {s_tbl}").fetchone()[0]
        config.cache_factor[s_tbl] = (config.cache_thres[s_tbl], max_degree)
        print(config.cache_factor[s_tbl])
    if config.enable_filter:
        # for i in range(config.label_range):
        #     for s_tbl in [f"group_sub_{i}", f"group_obj_{i}"]:
        #         config.cache_thres[s_tbl] = get_threshold(con, s_tbl)
        #         max_degree = con.execute(f"SELECT MAX(cnt) FROM {s_tbl}").fetchone()[0]
        #         config.cache_factor[s_tbl] = (config.cache_thres[s_tbl], max_degree)
        for s_tbl in [f"group_sub_f", f"group_obj_f"]:
            config.cache_thres[s_tbl] = get_threshold(con, s_tbl)
            max_degree = con.execute(f"SELECT MAX(cnt) FROM {s_tbl}").fetchone()[0]
            config.cache_factor[s_tbl] = (config.cache_thres[s_tbl], max_degree)
    end_time = time.time()
    print(f"get_threshold time: {end_time-start_time:.3f} seconds")


def initialize_joins(joins: List[Join], db_name: str):
    """Initialize join metadata and find triangle patterns."""
    for j in joins:
        if j.table1 not in config.variable_dict:
            config.variable_dict[j.table1] = set()
            config.column_dict[j.table1] = set()
        config.variable_dict[j.table1].add(j.label)
        config.column_dict[j.table1].add(j.attr1[0])
        config.attr_variable[f"{j.table1}.{j.attr1[0]}"] = j.label

        if j.table2 not in config.variable_dict:
            config.variable_dict[j.table2] = set()
            config.column_dict[j.table2] = set()
        config.variable_dict[j.table2].add(j.label)
        config.column_dict[j.table2].add(j.attr2[0])
        config.attr_variable[f"{j.table2}.{j.attr2[0]}"] = j.label

        config.original_table.add(j.table1)
        config.original_table.add(j.table2)
    
    find_triangles(joins)


def setup_join_factors(con, joins: List[Join], db_name: str):
    """Setup join selectivity factors based on heavy hitter analysis."""
    config.db_size = con.execute(f"SELECT COUNT(*) FROM {db_name};").fetchone()[0]
    
    for t in config.original_table:
        config.basetable_dict[t] = {t}
        config.table_size[t] = 1
        # if config.enable_filter:
        #     # config.table_sql[t] = f"(SELECT * FROM {db_name} WHERE label = {int(t[1:]) % config.label_range}) AS {t}"

        # else:
        config.table_sql[t] = f"{db_name} AS {t}"
        if config.enable_filter and t == "t1":
            config.table_sql[t] = config.filtered_table
        config.table_coe[t] = con.execute(f"SELECT COUNT(*) FROM {config.table_sql[t]}").fetchone()[0]
        # print(config.table_coe[t])
    for i, join in enumerate(joins):
        t0, t1 = int(join.table1[1:]) % config.label_range, int(join.table2[1:]) % config.label_range
        a0, a1 = join.attr1[0], join.attr2[0]
        if (not config.enable_filter) or (t0 != 1 and t1 != 1):
            s_tbl = ""
            if join.attr1[0] != join.attr2[0]:
                s_tbl = "sub_obj"
            elif join.attr1[0] == 'sub':
                s_tbl = "group_sub"
            else:
                s_tbl = "group_obj"
        else:
            s_tbl = f"sub_obj_{i}"
            if t1 == 1:
                t0, t1 = t1, t0
                a0, a1 = a1, a0
            con.execute(f"""
CREATE TEMP VIEW sub_obj_{i} AS (
    SELECT t1.val AS val, LEAST(t1.cnt, t2.cnt) AS cnt
    FROM group_{a0}_f AS t1 JOIN group_{a1} AS t2
    ON t1.val = t2.val
    ORDER BY cnt DESC
);""")
            config.umbra_sql += f"""
CREATE TEMP TABLE sub_obj_{i} AS (
    SELECT t1.val AS val, LEAST(t1.cnt, t2.cnt) AS cnt
    FROM group_{a0}_f AS t1 JOIN group_{a1} AS t2
    ON t1.val = t2.val
    ORDER BY cnt DESC
);"""
            config.cache_thres[s_tbl] = get_threshold(con, s_tbl)
            max_degree = con.execute(f"SELECT MAX(cnt) FROM {s_tbl}").fetchone()[0]
            config.cache_factor[s_tbl] = (config.cache_thres[s_tbl], max_degree)
            print(config.cache_factor[s_tbl])
        j_size_L, j_size = config.cache_factor[s_tbl]
        join.factor = j_size / config.db_size
        join.factor_L = j_size_L / config.db_size
        config.thres_dict[(join.table1, join.table2)] = (j_size_L, s_tbl)
        config.thres_dict[(join.table2, join.table1)] = (j_size_L, s_tbl)


def run_query(joins: List[Join], db_name: str):
    """Main query execution pipeline."""
    config.db_name = db_name
    initialize_joins(joins, db_name)
    
    con = duckdb.connect(database=':memory:conn')
    initialize_database(con, db_name)

    framework_time = time.time()
    setup_profiling(con)
    # need_split = precheck_thresholds(con, joins, db_name)
    need_split = True
    config.umbra_sql += f"""
\set repeat 4
\echo warmup begin:
SELECT COUNT(DISTINCT obj) as unique_objs
FROM {db_name};
SELECT COUNT(DISTINCT sub) as unique_subs
FROM {db_name};
\echo warmup end.
\echo preprocessing:
"""
    if config.db_name == "wgpb" or config.db_name == "gplus":
        config.filtered_table = f"""(SELECT sub, obj FROM {config.db_name} WHERE
    ascii(substring(md5(sub), 1, 1)) % 3 = 1) AS t1"""
        config.filtered_table_sampled = f"""(SELECT sub, obj FROM {config.db_name} WHERE 
hash(rowid) % 100 < {config.ratio} AND ascii(substring(md5(sub), 1, 1)) % 3 = 1) AS t1"""
    else:
        config.filtered_table = f"""(SELECT sub, obj FROM {config.db_name} WHERE sub % 3 = 1) AS t1"""
        config.filtered_table_sampled = f"""(SELECT sub, obj FROM {config.db_name} WHERE sub % 3 = 1 AND hash(rowid) % 100 < {config.ratio}) AS t1"""
    # config.umbra_sql += config.umbra_precheck_sql
    need_split = True
    # return
    if need_split:
        create_sampling_tables(con, db_name)
        if config.run_umbra_flag:
            create_sampling_tables_umbra(con, db_name)
        compute_thresholds(con)
        setup_join_factors(con, joins, db_name)
        create_j_list(joins)

    # sql = "SELECT COUNT(*) FROM "
    # for i, t in enumerate(sorted(config.original_table)):
    #     if i == 0:
    #         sql += f"{db_name} AS {t}"
    #     else:
    #         sql += f", {db_name} AS {t}"
    # sql += "\nWHERE "
    # for i, j in enumerate(joins):
    #     if i == 0:
    #         sql += f"{j.table1}.{j.attr1[0]} = {j.table2}.{j.attr2[0]}"
    #     else:
    #         sql += f" AND {j.table1}.{j.attr1[0]} = {j.table2}.{j.attr2[0]}"
    # if config.enable_filter:
    #     for t in config.original_table:
    #         sql += f" AND {t}.label = {int(t[1:]) % config.label_range}"
    # sql += ";"
    # config.final_joins = [sql]
    # print(sql)
    if not need_split or len(config.min_j_list) == 0:
        # Generate simple join query
        sql = "SELECT COUNT(*) FROM "
        for i, t in enumerate(sorted(config.original_table)):
            current_table = f"{db_name} AS {t}"
            if t == "t1" and config.enable_filter:
                current_table = config.filtered_table
            if i == 0:
                sql += current_table
            else:
                sql += f", {current_table}"
        sql += "\nWHERE "
        for i, j in enumerate(joins):
            if i == 0:
                sql += f"{j.table1}.{j.attr1[0]} = {j.table2}.{j.attr2[0]}"
            else:
                sql += f" AND {j.table1}.{j.attr1[0]} = {j.table2}.{j.attr2[0]}"
        # if config.enable_filter:
        #     for t in config.original_table:
        #         sql += f" AND {t}.label = {int(t[1:]) % config.label_range}"
        sql += ";"
        config.final_joins = [sql]
    else:
        # print("Split!")
        split_phase(con, copy.deepcopy(joins), 
                   np.zeros(len(joins), dtype=int), config.min_j_list)
    framework_time = time.time() - framework_time
    print(f"framework used time: {framework_time:.3f}")
    exe_final_join(con)
    print(f"total_time: {config.total_time + framework_time:.3f}")
    con.close()
    config.reset_globals()