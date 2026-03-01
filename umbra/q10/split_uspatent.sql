
\o -
\set timeout 5400000
\echo running q10 uspatent

\set repeat 4
\echo warmup begin:
SELECT 
    COUNT(*) as total_rows,
    COUNT(DISTINCT sub) as unique_subs,
    COUNT(DISTINCT obj) as unique_objs
FROM uspatent;
\echo warmup end.
\echo preprocessing:

CREATE TEMP TABLE group_obj AS (
    SELECT obj AS val, COUNT(*) AS cnt 
    FROM uspatent 
    GROUP BY obj 
    ORDER BY cnt DESC 
    LIMIT 10000
);

CREATE TEMP TABLE group_sub AS (
    SELECT sub AS val, COUNT(*) AS cnt 
    FROM uspatent 
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
EXPLAIN ANALYZE EXPLAIN ANALYZE SELECT COUNT(*) FROM uspatent AS t1, uspatent AS t2, uspatent AS t3, uspatent AS t4, uspatent AS t5, uspatent AS t6
WHERE t1.obj = t2.sub AND t2.obj = t3.sub AND t3.obj = t4.sub AND t4.obj = t5.sub AND t5.obj = t6.sub AND t6.obj = t1.sub;;