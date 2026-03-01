
\o -
\set timeout 5400000
\echo running q10 wgpb

\set repeat 4
\echo warmup begin:
SELECT 
    COUNT(*) as total_rows,
    COUNT(DISTINCT sub) as unique_subs,
    COUNT(DISTINCT obj) as unique_objs
FROM wgpb;
\echo warmup end.
\echo preprocessing:

CREATE TEMP TABLE group_obj AS (
    SELECT obj AS val, COUNT(*) AS cnt 
    FROM wgpb 
    GROUP BY obj 
    ORDER BY cnt DESC 
    LIMIT 10000
);

CREATE TEMP TABLE group_sub AS (
    SELECT sub AS val, COUNT(*) AS cnt 
    FROM wgpb 
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

SELECT COUNT(*)
FROM 
(SELECT intermediate_9.t3_obj AS intermediate_9_t3_obj,
       intermediate_9.intermediate_8_intermediate_2_sub AS intermediate_9_intermediate_8_intermediate_2_sub,
       intermediate_9.intermediate_8_intermediate_2_obj AS intermediate_9_intermediate_8_intermediate_2_obj,
       intermediate_9.intermediate_8_intermediate_0_obj AS intermediate_9_intermediate_8_intermediate_0_obj,
       intermediate_9.intermediate_8_intermediate_0_sub AS intermediate_9_intermediate_8_intermediate_0_sub,
       intermediate_9.t3_sub AS intermediate_9_t3_sub,
       intermediate_11.intermediate_10_intermediate_4_sub AS intermediate_11_intermediate_10_intermediate_4_sub,
       intermediate_11.intermediate_10_intermediate_4_obj AS intermediate_11_intermediate_10_intermediate_4_obj,
       intermediate_11.intermediate_10_intermediate_6_obj AS intermediate_11_intermediate_10_intermediate_6_obj,
       intermediate_11.intermediate_10_intermediate_6_sub AS intermediate_11_intermediate_10_intermediate_6_sub,
       intermediate_11.t6_sub AS intermediate_11_t6_sub,
       intermediate_11.t6_obj AS intermediate_11_t6_obj
FROM 
(SELECT intermediate_10.intermediate_6_sub AS intermediate_10_intermediate_6_sub,
       intermediate_10.intermediate_4_sub AS intermediate_10_intermediate_4_sub,
       intermediate_10.intermediate_6_obj AS intermediate_10_intermediate_6_obj,
       intermediate_10.intermediate_4_obj AS intermediate_10_intermediate_4_obj,
       t6.sub AS t6_sub,
       t6.obj AS t6_obj
FROM wgpb AS t6 JOIN 
(SELECT intermediate_4.sub AS intermediate_4_sub,
       intermediate_4.obj AS intermediate_4_obj,
       intermediate_6.sub AS intermediate_6_sub,
       intermediate_6.obj AS intermediate_6_obj
FROM 
    (SELECT t.* 
    FROM wgpb t
    LEFT JOIN sub_obj c ON t.sub = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 160) AS intermediate_6
     JOIN 
    (SELECT t.* 
    FROM wgpb t
    LEFT JOIN sub_obj c ON t.obj = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 160) AS intermediate_4
     ON forceorder(intermediate_4.obj = intermediate_6.sub)) AS intermediate_10 ON forceorder(intermediate_10.intermediate_6_obj = t6.sub)) AS intermediate_11 JOIN 
(SELECT intermediate_8.intermediate_2_sub AS intermediate_8_intermediate_2_sub,
       intermediate_8.intermediate_0_obj AS intermediate_8_intermediate_0_obj,
       intermediate_8.intermediate_0_sub AS intermediate_8_intermediate_0_sub,
       intermediate_8.intermediate_2_obj AS intermediate_8_intermediate_2_obj,
       t3.sub AS t3_sub,
       t3.obj AS t3_obj
FROM wgpb AS t3 JOIN 
(SELECT intermediate_0.sub AS intermediate_0_sub,
       intermediate_0.obj AS intermediate_0_obj,
       intermediate_2.sub AS intermediate_2_sub,
       intermediate_2.obj AS intermediate_2_obj
FROM 
    (SELECT t.* 
    FROM wgpb t
    LEFT JOIN sub_obj c ON t.sub = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 160) AS intermediate_2
     JOIN 
    (SELECT t.* 
    FROM wgpb t
    LEFT JOIN sub_obj c ON t.obj = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 160) AS intermediate_0
     ON forceorder(intermediate_0.obj = intermediate_2.sub)) AS intermediate_8 ON forceorder(intermediate_8.intermediate_2_obj = t3.sub)) AS intermediate_9 ON forceorder(intermediate_9.t3_obj = intermediate_11.intermediate_10_intermediate_4_sub AND intermediate_9.intermediate_8_intermediate_0_sub = intermediate_11.t6_obj)) AS intermediate_12
        ;
SELECT COUNT(*)
FROM 
(SELECT intermediate_14.intermediate_13_t3_obj AS intermediate_14_intermediate_13_t3_obj,
       intermediate_14.intermediate_13_intermediate_5_sub AS intermediate_14_intermediate_13_intermediate_5_sub,
       intermediate_14.intermediate_2_obj AS intermediate_14_intermediate_2_obj,
       intermediate_14.intermediate_13_t3_sub AS intermediate_14_intermediate_13_t3_sub,
       intermediate_14.intermediate_2_sub AS intermediate_14_intermediate_2_sub,
       intermediate_14.intermediate_13_intermediate_5_obj AS intermediate_14_intermediate_13_intermediate_5_obj,
       intermediate_16.intermediate_15_intermediate_7_sub AS intermediate_16_intermediate_15_intermediate_7_sub,
       intermediate_16.intermediate_0_obj AS intermediate_16_intermediate_0_obj,
       intermediate_16.intermediate_15_intermediate_7_obj AS intermediate_16_intermediate_15_intermediate_7_obj,
       intermediate_16.intermediate_15_t6_sub AS intermediate_16_intermediate_15_t6_sub,
       intermediate_16.intermediate_15_t6_obj AS intermediate_16_intermediate_15_t6_obj,
       intermediate_16.intermediate_0_sub AS intermediate_16_intermediate_0_sub
FROM 
(SELECT intermediate_0.sub AS intermediate_0_sub,
       intermediate_0.obj AS intermediate_0_obj,
       intermediate_15.intermediate_7_sub AS intermediate_15_intermediate_7_sub,
       intermediate_15.intermediate_7_obj AS intermediate_15_intermediate_7_obj,
       intermediate_15.t6_sub AS intermediate_15_t6_sub,
       intermediate_15.t6_obj AS intermediate_15_t6_obj
FROM 
    (SELECT t.* 
    FROM wgpb t
    LEFT JOIN sub_obj c ON t.obj = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 160) AS intermediate_0
     JOIN 
(SELECT intermediate_7.sub AS intermediate_7_sub,
       intermediate_7.obj AS intermediate_7_obj,
       t6.sub AS t6_sub,
       t6.obj AS t6_obj
FROM 
    (SELECT t.* 
    FROM wgpb t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 160) c
        ON t.sub = c.val) AS intermediate_7
     JOIN wgpb AS t6 ON forceorder(intermediate_7.obj = t6.sub)) AS intermediate_15 ON forceorder(intermediate_0.sub = intermediate_15.t6_obj)) AS intermediate_16 JOIN 
(SELECT intermediate_2.sub AS intermediate_2_sub,
       intermediate_2.obj AS intermediate_2_obj,
       intermediate_13.intermediate_5_sub AS intermediate_13_intermediate_5_sub,
       intermediate_13.t3_obj AS intermediate_13_t3_obj,
       intermediate_13.intermediate_5_obj AS intermediate_13_intermediate_5_obj,
       intermediate_13.t3_sub AS intermediate_13_t3_sub
FROM 
    (SELECT t.* 
    FROM wgpb t
    LEFT JOIN sub_obj c ON t.sub = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 160) AS intermediate_2
     JOIN 
(SELECT t3.sub AS t3_sub,
       t3.obj AS t3_obj,
       intermediate_5.sub AS intermediate_5_sub,
       intermediate_5.obj AS intermediate_5_obj
FROM 
    (SELECT t.* 
    FROM wgpb t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 160) c
        ON t.obj = c.val) AS intermediate_5
     JOIN wgpb AS t3 ON forceorder(t3.obj = intermediate_5.sub)) AS intermediate_13 ON forceorder(intermediate_2.obj = intermediate_13.t3_sub)) AS intermediate_14 ON forceorder(intermediate_14.intermediate_2_sub = intermediate_16.intermediate_0_obj AND intermediate_14.intermediate_13_intermediate_5_obj = intermediate_16.intermediate_15_intermediate_7_sub)) AS intermediate_17
        ;
SELECT COUNT(*)
FROM 
(SELECT intermediate_23.intermediate_18_sub AS intermediate_23_intermediate_18_sub,
       intermediate_23.intermediate_18_obj AS intermediate_23_intermediate_18_obj,
       intermediate_23.intermediate_22_t3_obj AS intermediate_23_intermediate_22_t3_obj,
       intermediate_23.intermediate_22_intermediate_3_sub AS intermediate_23_intermediate_22_intermediate_3_sub,
       intermediate_23.intermediate_22_intermediate_3_obj AS intermediate_23_intermediate_22_intermediate_3_obj,
       intermediate_23.intermediate_22_t3_sub AS intermediate_23_intermediate_22_t3_sub,
       intermediate_25.intermediate_24_t6_sub AS intermediate_25_intermediate_24_t6_sub,
       intermediate_25.intermediate_20_obj AS intermediate_25_intermediate_20_obj,
       intermediate_25.intermediate_24_intermediate_1_sub AS intermediate_25_intermediate_24_intermediate_1_sub,
       intermediate_25.intermediate_24_t6_obj AS intermediate_25_intermediate_24_t6_obj,
       intermediate_25.intermediate_20_sub AS intermediate_25_intermediate_20_sub,
       intermediate_25.intermediate_24_intermediate_1_obj AS intermediate_25_intermediate_24_intermediate_1_obj
FROM 
(SELECT intermediate_20.sub AS intermediate_20_sub,
       intermediate_20.obj AS intermediate_20_obj,
       intermediate_24.t6_sub AS intermediate_24_t6_sub,
       intermediate_24.intermediate_1_sub AS intermediate_24_intermediate_1_sub,
       intermediate_24.intermediate_1_obj AS intermediate_24_intermediate_1_obj,
       intermediate_24.t6_obj AS intermediate_24_t6_obj
FROM 
    (SELECT t.* 
    FROM wgpb t
    LEFT JOIN sub_obj c ON t.sub = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 160) AS intermediate_20
     JOIN 
(SELECT intermediate_1.sub AS intermediate_1_sub,
       intermediate_1.obj AS intermediate_1_obj,
       t6.sub AS t6_sub,
       t6.obj AS t6_obj
FROM 
    (SELECT t.* 
    FROM wgpb t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 160) c
        ON t.obj = c.val) AS intermediate_1
     JOIN wgpb AS t6 ON forceorder(intermediate_1.sub = t6.obj)) AS intermediate_24 ON forceorder(intermediate_20.obj = intermediate_24.t6_sub)) AS intermediate_25 JOIN 
(SELECT intermediate_22.intermediate_3_sub AS intermediate_22_intermediate_3_sub,
       intermediate_22.t3_obj AS intermediate_22_t3_obj,
       intermediate_22.intermediate_3_obj AS intermediate_22_intermediate_3_obj,
       intermediate_22.t3_sub AS intermediate_22_t3_sub,
       intermediate_18.sub AS intermediate_18_sub,
       intermediate_18.obj AS intermediate_18_obj
FROM 
    (SELECT t.* 
    FROM wgpb t
    LEFT JOIN sub_obj c ON t.obj = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 160) AS intermediate_18
     JOIN 
(SELECT intermediate_3.sub AS intermediate_3_sub,
       intermediate_3.obj AS intermediate_3_obj,
       t3.sub AS t3_sub,
       t3.obj AS t3_obj
FROM 
    (SELECT t.* 
    FROM wgpb t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 160) c
        ON t.sub = c.val) AS intermediate_3
     JOIN wgpb AS t3 ON forceorder(intermediate_3.obj = t3.sub)) AS intermediate_22 ON forceorder(intermediate_22.t3_obj = intermediate_18.sub)) AS intermediate_23 ON forceorder(intermediate_23.intermediate_22_intermediate_3_sub = intermediate_25.intermediate_24_intermediate_1_obj AND intermediate_23.intermediate_18_obj = intermediate_25.intermediate_20_sub)) AS intermediate_26
        ;
SELECT COUNT(*)
FROM 
(SELECT intermediate_28.intermediate_27_t3_obj AS intermediate_28_intermediate_27_t3_obj,
       intermediate_28.intermediate_27_intermediate_19_obj AS intermediate_28_intermediate_27_intermediate_19_obj,
       intermediate_28.intermediate_3_sub AS intermediate_28_intermediate_3_sub,
       intermediate_28.intermediate_27_intermediate_19_sub AS intermediate_28_intermediate_27_intermediate_19_sub,
       intermediate_28.intermediate_27_t3_sub AS intermediate_28_intermediate_27_t3_sub,
       intermediate_28.intermediate_3_obj AS intermediate_28_intermediate_3_obj,
       intermediate_30.intermediate_29_intermediate_21_sub AS intermediate_30_intermediate_29_intermediate_21_sub,
       intermediate_30.intermediate_1_sub AS intermediate_30_intermediate_1_sub,
       intermediate_30.intermediate_29_t6_obj AS intermediate_30_intermediate_29_t6_obj,
       intermediate_30.intermediate_1_obj AS intermediate_30_intermediate_1_obj,
       intermediate_30.intermediate_29_intermediate_21_obj AS intermediate_30_intermediate_29_intermediate_21_obj,
       intermediate_30.intermediate_29_t6_sub AS intermediate_30_intermediate_29_t6_sub
FROM 
(SELECT intermediate_1.sub AS intermediate_1_sub,
       intermediate_1.obj AS intermediate_1_obj,
       intermediate_29.intermediate_21_obj AS intermediate_29_intermediate_21_obj,
       intermediate_29.t6_sub AS intermediate_29_t6_sub,
       intermediate_29.intermediate_21_sub AS intermediate_29_intermediate_21_sub,
       intermediate_29.t6_obj AS intermediate_29_t6_obj
FROM 
    (SELECT t.* 
    FROM wgpb t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 160) c
        ON t.obj = c.val) AS intermediate_1
     JOIN 
(SELECT intermediate_21.sub AS intermediate_21_sub,
       intermediate_21.obj AS intermediate_21_obj,
       t6.sub AS t6_sub,
       t6.obj AS t6_obj
FROM 
    (SELECT t.* 
    FROM wgpb t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 160) c
        ON t.sub = c.val) AS intermediate_21
     JOIN wgpb AS t6 ON forceorder(intermediate_21.obj = t6.sub)) AS intermediate_29 ON forceorder(intermediate_1.sub = intermediate_29.t6_obj)) AS intermediate_30 JOIN 
(SELECT intermediate_3.sub AS intermediate_3_sub,
       intermediate_3.obj AS intermediate_3_obj,
       intermediate_27.intermediate_19_obj AS intermediate_27_intermediate_19_obj,
       intermediate_27.intermediate_19_sub AS intermediate_27_intermediate_19_sub,
       intermediate_27.t3_obj AS intermediate_27_t3_obj,
       intermediate_27.t3_sub AS intermediate_27_t3_sub
FROM 
    (SELECT t.* 
    FROM wgpb t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 160) c
        ON t.sub = c.val) AS intermediate_3
     JOIN 
(SELECT t3.sub AS t3_sub,
       t3.obj AS t3_obj,
       intermediate_19.sub AS intermediate_19_sub,
       intermediate_19.obj AS intermediate_19_obj
FROM 
    (SELECT t.* 
    FROM wgpb t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 160) c
        ON t.obj = c.val) AS intermediate_19
     JOIN wgpb AS t3 ON forceorder(t3.obj = intermediate_19.sub)) AS intermediate_27 ON forceorder(intermediate_3.obj = intermediate_27.t3_sub)) AS intermediate_28 ON forceorder(intermediate_28.intermediate_3_sub = intermediate_30.intermediate_1_obj AND intermediate_28.intermediate_27_intermediate_19_obj = intermediate_30.intermediate_29_intermediate_21_sub)) AS intermediate_31
        ;