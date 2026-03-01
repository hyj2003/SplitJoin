
\o -
\set timeout 5400000
\echo running q1 topcats

\set repeat 4
\echo warmup begin:
SELECT 
    COUNT(*) as total_rows,
    COUNT(DISTINCT sub) as unique_subs,
    COUNT(DISTINCT obj) as unique_objs
FROM topcats;
\echo warmup end.
\echo preprocessing:

CREATE TEMP TABLE group_obj AS (
    SELECT obj AS val, COUNT(*) AS cnt 
    FROM topcats 
    GROUP BY obj 
    ORDER BY cnt DESC 
    LIMIT 10000
);

CREATE TEMP TABLE group_sub AS (
    SELECT sub AS val, COUNT(*) AS cnt 
    FROM topcats 
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
EXPLAIN ANALYZE 
SELECT COUNT(*)
FROM 
(SELECT intermediate_4.intermediate_0_sub AS intermediate_4_intermediate_0_sub,
       intermediate_4.intermediate_2_obj AS intermediate_4_intermediate_2_obj,
       intermediate_4.intermediate_2_sub AS intermediate_4_intermediate_2_sub,
       intermediate_4.intermediate_0_obj AS intermediate_4_intermediate_0_obj,
       t3.sub AS t3_sub,
       t3.obj AS t3_obj
FROM topcats AS t3 JOIN 
(SELECT intermediate_0.sub AS intermediate_0_sub,
       intermediate_0.obj AS intermediate_0_obj,
       intermediate_2.sub AS intermediate_2_sub,
       intermediate_2.obj AS intermediate_2_obj
FROM 
    (SELECT t.* 
    FROM topcats t
    LEFT JOIN sub_obj c ON t.sub = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 323) AS intermediate_2
     JOIN 
    (SELECT t.* 
    FROM topcats t
    LEFT JOIN sub_obj c ON t.obj = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 323) AS intermediate_0
     ON forceorder(intermediate_0.obj = intermediate_2.sub)) AS intermediate_4 ON forceorder(intermediate_4.intermediate_2_obj = t3.obj AND intermediate_4.intermediate_0_sub = t3.sub)) AS intermediate_5
        ;EXPLAIN ANALYZE 
SELECT COUNT(*)
FROM 
(SELECT intermediate_1.sub AS intermediate_1_sub,
       intermediate_1.obj AS intermediate_1_obj,
       intermediate_6.intermediate_3_sub AS intermediate_6_intermediate_3_sub,
       intermediate_6.intermediate_3_obj AS intermediate_6_intermediate_3_obj,
       intermediate_6.t3_obj AS intermediate_6_t3_obj,
       intermediate_6.t3_sub AS intermediate_6_t3_sub
FROM 
    (SELECT t.* 
    FROM topcats t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 323) c
        ON t.obj = c.val) AS intermediate_1
     JOIN 
(SELECT intermediate_3.sub AS intermediate_3_sub,
       intermediate_3.obj AS intermediate_3_obj,
       t3.sub AS t3_sub,
       t3.obj AS t3_obj
FROM 
    (SELECT t.* 
    FROM topcats t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 323) c
        ON t.sub = c.val) AS intermediate_3
     JOIN topcats AS t3 ON forceorder(intermediate_3.obj = t3.obj)) AS intermediate_6 ON forceorder(intermediate_1.obj = intermediate_6.intermediate_3_sub AND intermediate_1.sub = intermediate_6.t3_sub)) AS intermediate_7
        ;