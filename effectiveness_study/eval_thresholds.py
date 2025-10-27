import duckdb
import time
import os
import math
databases = ['gplus', 'topcats']
os.sched_setaffinity(0, set(range(32)))
for db_name in databases:
    con = duckdb.connect(database=':memory:')
    con.execute(f"IMPORT DATABASE '{db_name}'")
    con.execute("SET threads = 32;")
    con.execute(f"""
CREATE TABLE table_obj AS (
    SELECT obj, COUNT(*) AS cnt
    FROM {db_name}
    GROUP BY obj
    ORDER BY cnt DESC
);
    """)
    con.execute(f"""
CREATE TABLE table_sub AS (
    SELECT sub, COUNT(*) AS cnt
    FROM {db_name}
    GROUP BY sub
    ORDER BY cnt DESC
);
""")
    obj_df = con.execute(f"""SELECT table_obj.* FROM table_obj""").fetchdf()
    sub_df = con.execute(f"""SELECT table_sub.* FROM table_sub""").fetchdf()
    # sum_obj, sum_sub = 0, 0
    sum_obj, sum_sub = obj_df['cnt'][0], sub_df['cnt'][0]
    # for cnt in obj_df['cnt']:
    #     sum_obj += cnt * cnt
    # for cnt in sub_df['cnt']:
    #     sum_sub += cnt * cnt
    df = obj_df if sum_obj < sum_sub else sub_df
    flag = sum_obj < sum_sub
    # flag2 = sum_obj < sum_sub
    # print(flag, flag2)
    # continue
    topk = 0
    for k, cnt in enumerate(df['cnt']):
        if k >= cnt:
            topk = cnt
            break
    k_lst = [0, topk]
    for i in range(0, 5):
        if 10 ** i >= len(df['cnt']):
            break
        k_lst.append(10 ** i)
    k_lst = sorted(k_lst)
    for k in k_lst:
        times_l, times_h = [], []
        threshold = 0 if k == len(df['cnt'])-1 else df['cnt'][k]
        con.execute("SET disabled_optimizers='join_order,build_side_probe_side'")
        for i in range(4):
            start = time.perf_counter()
    
            if flag:
                con.execute(f"""
SELECT COUNT(*)
FROM {db_name} t2 
JOIN (
    SELECT t.sub, t.obj
    FROM {db_name} t JOIN table_obj c ON t.obj = c.obj
    WHERE c.cnt <= {threshold}
) t3 ON t3.obj = t2.obj
JOIN {db_name} t1 ON t3.sub = t1.sub AND t1.obj = t2.sub
UNION ALL
SELECT COUNT(*)
FROM {db_name} t1 
JOIN (
    SELECT t.sub, t.obj
    FROM {db_name} t JOIN table_obj c ON t.obj = c.obj
    WHERE c.cnt > {threshold}
) t3 ON t1.sub = t3.sub
JOIN {db_name} t2 ON t1.obj = t2.sub AND t3.obj = t2.obj""")
            else:
                con.execute(f"""
SELECT COUNT(*)
FROM {db_name} t2 
JOIN (
    SELECT t.sub, t.obj
    FROM {db_name} t JOIN table_sub c ON t.sub = c.sub
    WHERE c.cnt > {threshold}
) t3 ON t3.obj = t2.obj
JOIN {db_name} t1 ON t3.sub = t1.sub AND t1.obj = t2.sub
UNION ALL
SELECT COUNT(*)
FROM {db_name} t1 
JOIN (
    SELECT t.sub, t.obj
    FROM {db_name} t JOIN table_sub c ON t.sub = c.sub
    WHERE c.cnt <= {threshold}
) t3 ON t1.sub = t3.sub
JOIN {db_name} t2 ON t1.obj = t2.sub AND t3.obj = t2.obj""")
            end = time.perf_counter()
            times_l.append(end - start)

        if flag:
            size_l, size_h = con.execute(f"""
SELECT COUNT(*)
FROM {db_name} t2 
JOIN (
    SELECT t.sub, t.obj
    FROM {db_name} t JOIN table_obj c ON t.obj = c.obj
    WHERE c.cnt <= {threshold}
) t3 ON t3.obj = t2.obj""").fetchall()[0][0], con.execute(f"""
SELECT COUNT(*)
FROM {db_name} t1 
JOIN (
    SELECT t.sub, t.obj
    FROM {db_name} t JOIN table_obj c ON t.obj = c.obj
    WHERE c.cnt > {threshold}
) t3 ON t1.sub = t3.sub
""").fetchall()[0][0]
        else:
            size_l, size_h = con.execute(f"""
SELECT COUNT(*)
FROM {db_name} t1 
JOIN (
    SELECT t.sub, t.obj
    FROM {db_name} t JOIN table_sub c ON t.sub = c.sub
    WHERE c.cnt <= {threshold}
) t3 ON t1.sub = t3.sub""").fetchall()[0][0], con.execute(f"""
SELECT COUNT(*)
FROM {db_name} t2 
JOIN (
    SELECT t.sub, t.obj
    FROM {db_name} t JOIN table_sub c ON t.sub = c.sub
    WHERE c.cnt > {threshold}
) t3 ON t3.obj = t2.obj""").fetchall()[0][0]
        if k == topk:
            print("Chosen Topk:", end="")
        print(f"Database={db_name}, num={k}")
        print("Time(s): ", min(times_l))
        print("Size(tuples): L, H, total:", size_l, size_h, max(size_l, size_h))
    con.close()