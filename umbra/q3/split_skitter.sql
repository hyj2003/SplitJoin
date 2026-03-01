
\o -
\set timeout 5400000
\echo running q3 skitter

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
EXPLAIN ANALYZE 
SELECT COUNT(*)
FROM 
(SELECT intermediate_4.intermediate_2_sub AS intermediate_4_intermediate_2_sub,
       intermediate_4.intermediate_0_sub AS intermediate_4_intermediate_0_sub,
       intermediate_4.intermediate_0_obj AS intermediate_4_intermediate_0_obj,
       intermediate_4.intermediate_2_obj AS intermediate_4_intermediate_2_obj,
       intermediate_5.t3_sub AS intermediate_5_t3_sub,
       intermediate_5.t4_obj AS intermediate_5_t4_obj,
       intermediate_5.t4_sub AS intermediate_5_t4_sub,
       intermediate_5.t3_obj AS intermediate_5_t3_obj
FROM 
(SELECT intermediate_0.obj AS intermediate_0_obj,
       intermediate_0.sub AS intermediate_0_sub,
       intermediate_2.obj AS intermediate_2_obj,
       intermediate_2.sub AS intermediate_2_sub
FROM 
    (SELECT t.* 
    FROM skitter t
    LEFT JOIN group_sub c ON t.sub = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 320) AS intermediate_2
     JOIN 
    (SELECT t.* 
    FROM skitter t
    LEFT JOIN group_sub c ON t.sub = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 320) AS intermediate_0
     ON forceorder(intermediate_0.sub = intermediate_2.sub)) AS intermediate_4 JOIN 
(SELECT t3.obj AS t3_obj,
       t3.sub AS t3_sub,
       t4.obj AS t4_obj,
       t4.sub AS t4_sub
FROM skitter AS t4 JOIN skitter AS t3 ON forceorder(t3.obj = t4.sub)) AS intermediate_5 ON forceorder(intermediate_4.intermediate_0_obj = intermediate_5.t3_sub AND intermediate_4.intermediate_2_obj = intermediate_5.t4_obj)) AS intermediate_6
        ;EXPLAIN ANALYZE 
SELECT COUNT(*)
FROM 
(SELECT intermediate_7.t3_obj AS intermediate_7_t3_obj,
       intermediate_7.t3_sub AS intermediate_7_t3_sub,
       intermediate_7.intermediate_1_obj AS intermediate_7_intermediate_1_obj,
       intermediate_7.intermediate_1_sub AS intermediate_7_intermediate_1_sub,
       intermediate_8.intermediate_3_sub AS intermediate_8_intermediate_3_sub,
       intermediate_8.t4_obj AS intermediate_8_t4_obj,
       intermediate_8.intermediate_3_obj AS intermediate_8_intermediate_3_obj,
       intermediate_8.t4_sub AS intermediate_8_t4_sub
FROM 
(SELECT intermediate_1.obj AS intermediate_1_obj,
       intermediate_1.sub AS intermediate_1_sub,
       t3.obj AS t3_obj,
       t3.sub AS t3_sub
FROM 
    (SELECT t.* 
    FROM skitter t
    JOIN (
        SELECT s.*
        FROM group_sub s
        WHERE s.cnt > 320) c
        ON t.sub = c.val) AS intermediate_1
     JOIN skitter AS t3 ON forceorder(intermediate_1.obj = t3.sub)) AS intermediate_7 JOIN 
(SELECT intermediate_3.obj AS intermediate_3_obj,
       intermediate_3.sub AS intermediate_3_sub,
       t4.obj AS t4_obj,
       t4.sub AS t4_sub
FROM 
    (SELECT t.* 
    FROM skitter t
    JOIN (
        SELECT s.*
        FROM group_sub s
        WHERE s.cnt > 320) c
        ON t.sub = c.val) AS intermediate_3
     JOIN skitter AS t4 ON forceorder(intermediate_3.obj = t4.obj)) AS intermediate_8 ON forceorder(intermediate_7.intermediate_1_sub = intermediate_8.intermediate_3_sub AND intermediate_7.t3_obj = intermediate_8.t4_sub)) AS intermediate_9
        ;