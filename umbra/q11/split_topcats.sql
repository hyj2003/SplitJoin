
\o -
\set timeout 5400000
\echo running q11 topcats

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
(SELECT intermediate_8.intermediate_0_obj AS intermediate_8_intermediate_0_obj,
       intermediate_8.intermediate_2_sub AS intermediate_8_intermediate_2_sub,
       intermediate_8.intermediate_2_obj AS intermediate_8_intermediate_2_obj,
       intermediate_8.intermediate_0_sub AS intermediate_8_intermediate_0_sub,
       intermediate_10.t5_sub AS intermediate_10_t5_sub,
       intermediate_10.t5_obj AS intermediate_10_t5_obj,
       intermediate_10.intermediate_9_intermediate_6_obj AS intermediate_10_intermediate_9_intermediate_6_obj,
       intermediate_10.intermediate_9_intermediate_6_sub AS intermediate_10_intermediate_9_intermediate_6_sub,
       intermediate_10.intermediate_9_intermediate_4_sub AS intermediate_10_intermediate_9_intermediate_4_sub,
       intermediate_10.intermediate_9_intermediate_4_obj AS intermediate_10_intermediate_9_intermediate_4_obj
FROM 
(SELECT intermediate_0.sub AS intermediate_0_sub,
       intermediate_0.obj AS intermediate_0_obj,
       intermediate_2.obj AS intermediate_2_obj,
       intermediate_2.sub AS intermediate_2_sub
FROM 
    (SELECT t.* 
    FROM topcats t
    LEFT JOIN sub_obj c ON t.obj = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 323) AS intermediate_2
     JOIN 
    (SELECT t.* 
    FROM topcats t
    LEFT JOIN sub_obj c ON t.sub = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 323) AS intermediate_0
     ON forceorder(intermediate_0.sub = intermediate_2.obj)) AS intermediate_8 JOIN 
(SELECT intermediate_9.intermediate_4_sub AS intermediate_9_intermediate_4_sub,
       intermediate_9.intermediate_6_sub AS intermediate_9_intermediate_6_sub,
       intermediate_9.intermediate_6_obj AS intermediate_9_intermediate_6_obj,
       intermediate_9.intermediate_4_obj AS intermediate_9_intermediate_4_obj,
       t5.obj AS t5_obj,
       t5.sub AS t5_sub
FROM topcats AS t5 JOIN 
(SELECT intermediate_4.sub AS intermediate_4_sub,
       intermediate_4.obj AS intermediate_4_obj,
       intermediate_6.sub AS intermediate_6_sub,
       intermediate_6.obj AS intermediate_6_obj
FROM 
    (SELECT t.* 
    FROM topcats t
    LEFT JOIN sub_obj c ON t.sub = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 323) AS intermediate_6
     JOIN 
    (SELECT t.* 
    FROM topcats t
    LEFT JOIN sub_obj c ON t.obj = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 323) AS intermediate_4
     ON forceorder(intermediate_4.obj = intermediate_6.sub)) AS intermediate_9 ON forceorder(intermediate_9.intermediate_6_obj = t5.sub)) AS intermediate_10 ON forceorder(intermediate_8.intermediate_0_obj = intermediate_10.intermediate_9_intermediate_4_sub AND intermediate_8.intermediate_2_sub = intermediate_10.t5_obj)) AS intermediate_11
        ;EXPLAIN ANALYZE 
SELECT COUNT(*)
FROM 
(SELECT intermediate_13.intermediate_2_sub AS intermediate_13_intermediate_2_sub,
       intermediate_13.intermediate_12_intermediate_5_obj AS intermediate_13_intermediate_12_intermediate_5_obj,
       intermediate_13.intermediate_12_intermediate_0_obj AS intermediate_13_intermediate_12_intermediate_0_obj,
       intermediate_13.intermediate_2_obj AS intermediate_13_intermediate_2_obj,
       intermediate_13.intermediate_12_intermediate_5_sub AS intermediate_13_intermediate_12_intermediate_5_sub,
       intermediate_13.intermediate_12_intermediate_0_sub AS intermediate_13_intermediate_12_intermediate_0_sub,
       intermediate_14.t5_obj AS intermediate_14_t5_obj,
       intermediate_14.intermediate_7_sub AS intermediate_14_intermediate_7_sub,
       intermediate_14.intermediate_7_obj AS intermediate_14_intermediate_7_obj,
       intermediate_14.t5_sub AS intermediate_14_t5_sub
FROM 
(SELECT intermediate_7.sub AS intermediate_7_sub,
       intermediate_7.obj AS intermediate_7_obj,
       t5.obj AS t5_obj,
       t5.sub AS t5_sub
FROM 
    (SELECT t.* 
    FROM topcats t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 323) c
        ON t.sub = c.val) AS intermediate_7
     JOIN topcats AS t5 ON forceorder(intermediate_7.obj = t5.sub)) AS intermediate_14 JOIN 
(SELECT intermediate_2.obj AS intermediate_2_obj,
       intermediate_2.sub AS intermediate_2_sub,
       intermediate_12.intermediate_0_obj AS intermediate_12_intermediate_0_obj,
       intermediate_12.intermediate_5_obj AS intermediate_12_intermediate_5_obj,
       intermediate_12.intermediate_5_sub AS intermediate_12_intermediate_5_sub,
       intermediate_12.intermediate_0_sub AS intermediate_12_intermediate_0_sub
FROM 
    (SELECT t.* 
    FROM topcats t
    LEFT JOIN sub_obj c ON t.obj = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 323) AS intermediate_2
     JOIN 
(SELECT intermediate_0.sub AS intermediate_0_sub,
       intermediate_0.obj AS intermediate_0_obj,
       intermediate_5.sub AS intermediate_5_sub,
       intermediate_5.obj AS intermediate_5_obj
FROM 
    (SELECT t.* 
    FROM topcats t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 323) c
        ON t.obj = c.val) AS intermediate_5
     JOIN 
    (SELECT t.* 
    FROM topcats t
    LEFT JOIN sub_obj c ON t.sub = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 323) AS intermediate_0
     ON forceorder(intermediate_0.obj = intermediate_5.sub)) AS intermediate_12 ON forceorder(intermediate_2.obj = intermediate_12.intermediate_0_sub)) AS intermediate_13 ON forceorder(intermediate_13.intermediate_12_intermediate_5_obj = intermediate_14.intermediate_7_sub AND intermediate_13.intermediate_2_sub = intermediate_14.t5_obj)) AS intermediate_15
        ;EXPLAIN ANALYZE 
SELECT COUNT(*)
FROM 
(SELECT intermediate_21.intermediate_20_intermediate_16_obj AS intermediate_21_intermediate_20_intermediate_16_obj,
       intermediate_21.intermediate_18_obj AS intermediate_21_intermediate_18_obj,
       intermediate_21.intermediate_20_intermediate_1_obj AS intermediate_21_intermediate_20_intermediate_1_obj,
       intermediate_21.intermediate_18_sub AS intermediate_21_intermediate_18_sub,
       intermediate_21.intermediate_20_intermediate_16_sub AS intermediate_21_intermediate_20_intermediate_16_sub,
       intermediate_21.intermediate_20_intermediate_1_sub AS intermediate_21_intermediate_20_intermediate_1_sub,
       intermediate_22.t5_obj AS intermediate_22_t5_obj,
       intermediate_22.intermediate_3_obj AS intermediate_22_intermediate_3_obj,
       intermediate_22.intermediate_3_sub AS intermediate_22_intermediate_3_sub,
       intermediate_22.t5_sub AS intermediate_22_t5_sub
FROM 
(SELECT intermediate_3.obj AS intermediate_3_obj,
       intermediate_3.sub AS intermediate_3_sub,
       t5.obj AS t5_obj,
       t5.sub AS t5_sub
FROM 
    (SELECT t.* 
    FROM topcats t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 323) c
        ON t.obj = c.val) AS intermediate_3
     JOIN topcats AS t5 ON forceorder(intermediate_3.sub = t5.obj)) AS intermediate_22 JOIN 
(SELECT intermediate_20.intermediate_1_obj AS intermediate_20_intermediate_1_obj,
       intermediate_20.intermediate_16_obj AS intermediate_20_intermediate_16_obj,
       intermediate_20.intermediate_1_sub AS intermediate_20_intermediate_1_sub,
       intermediate_20.intermediate_16_sub AS intermediate_20_intermediate_16_sub,
       intermediate_18.sub AS intermediate_18_sub,
       intermediate_18.obj AS intermediate_18_obj
FROM 
    (SELECT t.* 
    FROM topcats t
    LEFT JOIN sub_obj c ON t.sub = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 323) AS intermediate_18
     JOIN 
(SELECT intermediate_1.sub AS intermediate_1_sub,
       intermediate_1.obj AS intermediate_1_obj,
       intermediate_16.sub AS intermediate_16_sub,
       intermediate_16.obj AS intermediate_16_obj
FROM 
    (SELECT t.* 
    FROM topcats t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 323) c
        ON t.sub = c.val) AS intermediate_1
     JOIN 
    (SELECT t.* 
    FROM topcats t
    LEFT JOIN sub_obj c ON t.obj = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 323) AS intermediate_16
     ON forceorder(intermediate_1.obj = intermediate_16.sub)) AS intermediate_20 ON forceorder(intermediate_20.intermediate_16_obj = intermediate_18.sub)) AS intermediate_21 ON forceorder(intermediate_21.intermediate_20_intermediate_1_sub = intermediate_22.intermediate_3_obj AND intermediate_21.intermediate_18_obj = intermediate_22.t5_sub)) AS intermediate_23
        ;EXPLAIN ANALYZE 
SELECT COUNT(*)
FROM 
(SELECT intermediate_25.intermediate_24_intermediate_19_obj AS intermediate_25_intermediate_24_intermediate_19_obj,
       intermediate_25.intermediate_3_sub AS intermediate_25_intermediate_3_sub,
       intermediate_25.intermediate_24_intermediate_19_sub AS intermediate_25_intermediate_24_intermediate_19_sub,
       intermediate_25.intermediate_24_t5_sub AS intermediate_25_intermediate_24_t5_sub,
       intermediate_25.intermediate_3_obj AS intermediate_25_intermediate_3_obj,
       intermediate_25.intermediate_24_t5_obj AS intermediate_25_intermediate_24_t5_obj,
       intermediate_26.intermediate_1_obj AS intermediate_26_intermediate_1_obj,
       intermediate_26.intermediate_17_sub AS intermediate_26_intermediate_17_sub,
       intermediate_26.intermediate_1_sub AS intermediate_26_intermediate_1_sub,
       intermediate_26.intermediate_17_obj AS intermediate_26_intermediate_17_obj
FROM 
(SELECT intermediate_1.sub AS intermediate_1_sub,
       intermediate_1.obj AS intermediate_1_obj,
       intermediate_17.sub AS intermediate_17_sub,
       intermediate_17.obj AS intermediate_17_obj
FROM 
    (SELECT t.* 
    FROM topcats t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 323) c
        ON t.obj = c.val) AS intermediate_17
     JOIN 
    (SELECT t.* 
    FROM topcats t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 323) c
        ON t.sub = c.val) AS intermediate_1
     ON forceorder(intermediate_1.obj = intermediate_17.sub)) AS intermediate_26 JOIN 
(SELECT intermediate_3.obj AS intermediate_3_obj,
       intermediate_3.sub AS intermediate_3_sub,
       intermediate_24.t5_sub AS intermediate_24_t5_sub,
       intermediate_24.t5_obj AS intermediate_24_t5_obj,
       intermediate_24.intermediate_19_sub AS intermediate_24_intermediate_19_sub,
       intermediate_24.intermediate_19_obj AS intermediate_24_intermediate_19_obj
FROM 
    (SELECT t.* 
    FROM topcats t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 323) c
        ON t.obj = c.val) AS intermediate_3
     JOIN 
(SELECT intermediate_19.sub AS intermediate_19_sub,
       intermediate_19.obj AS intermediate_19_obj,
       t5.obj AS t5_obj,
       t5.sub AS t5_sub
FROM 
    (SELECT t.* 
    FROM topcats t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 323) c
        ON t.sub = c.val) AS intermediate_19
     JOIN topcats AS t5 ON forceorder(intermediate_19.obj = t5.sub)) AS intermediate_24 ON forceorder(intermediate_3.sub = intermediate_24.t5_obj)) AS intermediate_25 ON forceorder(intermediate_25.intermediate_3_obj = intermediate_26.intermediate_1_sub AND intermediate_25.intermediate_24_intermediate_19_sub = intermediate_26.intermediate_17_obj)) AS intermediate_27
        ;