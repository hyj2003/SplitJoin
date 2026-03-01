
\o -
\set timeout 5400000
\echo running q1_f uspatent

\set repeat 4
\echo warmup begin:
SELECT COUNT(DISTINCT obj) as unique_objs
FROM uspatent;
SELECT COUNT(DISTINCT sub) as unique_subs
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

CREATE TEMP TABLE group_obj_f AS (
    SELECT obj AS val, COUNT(*) AS cnt 
    FROM (SELECT sub, obj FROM uspatent WHERE sub % 3 = 1) AS t1 
    GROUP BY obj 
    ORDER BY cnt DESC 
    LIMIT 5000
);

CREATE TEMP TABLE group_sub_f AS (
    SELECT sub AS val, COUNT(*) AS cnt 
    FROM (SELECT sub, obj FROM uspatent WHERE sub % 3 = 1) AS t1 
    GROUP BY sub 
    ORDER BY cnt DESC 
    LIMIT 5000
);

CREATE TEMP TABLE sub_obj_0 AS (
    SELECT t1.val AS val, LEAST(t1.cnt, t2.cnt) AS cnt
    FROM group_obj_f AS t1 JOIN group_sub AS t2
    ON t1.val = t2.val
    ORDER BY cnt DESC
);
CREATE TEMP TABLE sub_obj_2 AS (
    SELECT t1.val AS val, LEAST(t1.cnt, t2.cnt) AS cnt
    FROM group_sub_f AS t1 JOIN group_sub AS t2
    ON t1.val = t2.val
    ORDER BY cnt DESC
);
\echo join time:
SELECT COUNT(*) FROM (SELECT sub, obj FROM uspatent WHERE sub % 3 = 1) AS t1, uspatent AS t2, uspatent AS t3
WHERE t1.obj = t2.sub AND t2.obj = t3.obj AND t3.sub = t1.sub;;