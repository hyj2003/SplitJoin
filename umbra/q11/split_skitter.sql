
\o -
\set timeout 5400000
\echo running q11 skitter

\set repeat 4
\echo warmup begin:
SELECT 
    COUNT(*) as total_rows,
    COUNT(DISTINCT sub) as unique_subs,
    COUNT(DISTINCT obj) as unique_objs
FROM skitter;
\echo warmup end.
\echo preprocessing:

CREATE TEMP TABLE group_obj AS (
    SELECT obj AS val, COUNT(*) AS cnt 
    FROM skitter 
    GROUP BY obj 
    ORDER BY cnt DESC 
    LIMIT 10000
);

CREATE TEMP TABLE group_sub AS (
    SELECT sub AS val, COUNT(*) AS cnt 
    FROM skitter 
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

\echo join time:
EXPLAIN ANALYZE EXPLAIN ANALYZE SELECT COUNT(*) FROM skitter AS t1, skitter AS t2, skitter AS t3, skitter AS t4, skitter AS t5
WHERE t2.sub = t1.obj AND t2.obj = t3.sub AND t3.obj = t4.sub AND t4.obj = t5.sub AND t1.sub = t5.obj;;