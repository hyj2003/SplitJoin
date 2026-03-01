
\o -
\set timeout 5400000
\echo running q8 gplus

\set repeat 4
\echo warmup begin:
SELECT 
    COUNT(*) as total_rows,
    COUNT(DISTINCT sub) as unique_subs,
    COUNT(DISTINCT obj) as unique_objs
FROM gplus;
\echo warmup end.
\echo preprocessing:

CREATE TEMP TABLE group_obj AS (
    SELECT obj AS val, COUNT(*) AS cnt 
    FROM gplus 
    GROUP BY obj 
    ORDER BY cnt DESC 
    LIMIT 10000
);

CREATE TEMP TABLE group_sub AS (
    SELECT sub AS val, COUNT(*) AS cnt 
    FROM gplus 
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
(SELECT intermediate_14.intermediate_13_t3_sub AS intermediate_14_intermediate_13_t3_sub,
       intermediate_14.t8_sub AS intermediate_14_t8_sub,
       intermediate_14.intermediate_13_intermediate_12_intermediate_0_obj AS intermediate_14_intermediate_13_intermediate_12_intermediate_0_obj,
       intermediate_14.intermediate_13_intermediate_12_intermediate_2_sub AS intermediate_14_intermediate_13_intermediate_12_intermediate_2_sub,
       intermediate_14.intermediate_13_intermediate_12_intermediate_0_sub AS intermediate_14_intermediate_13_intermediate_12_intermediate_0_sub,
       intermediate_14.t8_obj AS intermediate_14_t8_obj,
       intermediate_14.intermediate_13_t3_obj AS intermediate_14_intermediate_13_t3_obj,
       intermediate_14.intermediate_13_intermediate_12_intermediate_2_obj AS intermediate_14_intermediate_13_intermediate_12_intermediate_2_obj,
       intermediate_17.intermediate_10_obj AS intermediate_17_intermediate_10_obj,
       intermediate_17.intermediate_16_intermediate_8_sub AS intermediate_17_intermediate_16_intermediate_8_sub,
       intermediate_17.intermediate_16_intermediate_15_intermediate_6_obj AS intermediate_17_intermediate_16_intermediate_15_intermediate_6_obj,
       intermediate_17.intermediate_16_intermediate_15_intermediate_6_sub AS intermediate_17_intermediate_16_intermediate_15_intermediate_6_sub,
       intermediate_17.intermediate_10_sub AS intermediate_17_intermediate_10_sub,
       intermediate_17.intermediate_16_intermediate_15_intermediate_4_obj AS intermediate_17_intermediate_16_intermediate_15_intermediate_4_obj,
       intermediate_17.intermediate_16_intermediate_8_obj AS intermediate_17_intermediate_16_intermediate_8_obj,
       intermediate_17.intermediate_16_intermediate_15_intermediate_4_sub AS intermediate_17_intermediate_16_intermediate_15_intermediate_4_sub
FROM 
(SELECT intermediate_16.intermediate_8_sub AS intermediate_16_intermediate_8_sub,
       intermediate_16.intermediate_15_intermediate_6_obj AS intermediate_16_intermediate_15_intermediate_6_obj,
       intermediate_16.intermediate_15_intermediate_4_sub AS intermediate_16_intermediate_15_intermediate_4_sub,
       intermediate_16.intermediate_15_intermediate_6_sub AS intermediate_16_intermediate_15_intermediate_6_sub,
       intermediate_16.intermediate_8_obj AS intermediate_16_intermediate_8_obj,
       intermediate_16.intermediate_15_intermediate_4_obj AS intermediate_16_intermediate_15_intermediate_4_obj,
       intermediate_10.sub AS intermediate_10_sub,
       intermediate_10.obj AS intermediate_10_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.obj = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_10
     JOIN 
(SELECT intermediate_8.sub AS intermediate_8_sub,
       intermediate_8.obj AS intermediate_8_obj,
       intermediate_15.intermediate_6_obj AS intermediate_15_intermediate_6_obj,
       intermediate_15.intermediate_6_sub AS intermediate_15_intermediate_6_sub,
       intermediate_15.intermediate_4_sub AS intermediate_15_intermediate_4_sub,
       intermediate_15.intermediate_4_obj AS intermediate_15_intermediate_4_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.sub = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_8
     JOIN 
(SELECT intermediate_4.sub AS intermediate_4_sub,
       intermediate_4.obj AS intermediate_4_obj,
       intermediate_6.sub AS intermediate_6_sub,
       intermediate_6.obj AS intermediate_6_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.obj = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_6
     JOIN 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.sub = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_4
     ON forceorder(intermediate_4.sub = intermediate_6.obj)) AS intermediate_15 ON forceorder(intermediate_8.sub = intermediate_15.intermediate_4_obj AND intermediate_8.obj = intermediate_15.intermediate_6_sub)) AS intermediate_16 ON forceorder(intermediate_16.intermediate_8_sub = intermediate_10.obj)) AS intermediate_17 JOIN 
(SELECT intermediate_13.intermediate_12_intermediate_2_sub AS intermediate_13_intermediate_12_intermediate_2_sub,
       intermediate_13.intermediate_12_intermediate_0_sub AS intermediate_13_intermediate_12_intermediate_0_sub,
       intermediate_13.intermediate_12_intermediate_2_obj AS intermediate_13_intermediate_12_intermediate_2_obj,
       intermediate_13.intermediate_12_intermediate_0_obj AS intermediate_13_intermediate_12_intermediate_0_obj,
       intermediate_13.t3_obj AS intermediate_13_t3_obj,
       intermediate_13.t3_sub AS intermediate_13_t3_sub,
       t8.sub AS t8_sub,
       t8.obj AS t8_obj
FROM gplus AS t8 JOIN 
(SELECT intermediate_12.intermediate_2_sub AS intermediate_12_intermediate_2_sub,
       intermediate_12.intermediate_2_obj AS intermediate_12_intermediate_2_obj,
       intermediate_12.intermediate_0_obj AS intermediate_12_intermediate_0_obj,
       intermediate_12.intermediate_0_sub AS intermediate_12_intermediate_0_sub,
       t3.sub AS t3_sub,
       t3.obj AS t3_obj
FROM gplus AS t3 JOIN 
(SELECT intermediate_0.sub AS intermediate_0_sub,
       intermediate_0.obj AS intermediate_0_obj,
       intermediate_2.sub AS intermediate_2_sub,
       intermediate_2.obj AS intermediate_2_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.sub = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_2
     JOIN 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.obj = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_0
     ON forceorder(intermediate_0.obj = intermediate_2.sub)) AS intermediate_12 ON forceorder(intermediate_12.intermediate_0_sub = t3.obj AND intermediate_12.intermediate_2_obj = t3.sub)) AS intermediate_13 ON forceorder(intermediate_13.intermediate_12_intermediate_2_sub = t8.obj)) AS intermediate_14 ON forceorder(intermediate_14.t8_sub = intermediate_17.intermediate_10_sub AND intermediate_14.intermediate_13_intermediate_12_intermediate_2_obj = intermediate_17.intermediate_16_intermediate_8_obj)) AS intermediate_18
        ;EXPLAIN ANALYZE 
SELECT COUNT(*)
FROM 
(SELECT intermediate_21.intermediate_20_intermediate_19_intermediate_0_sub AS intermediate_21_intermediate_20_intermediate_19_intermediate_0_sub,
       intermediate_21.intermediate_20_intermediate_19_intermediate_2_sub AS intermediate_21_intermediate_20_intermediate_19_intermediate_2_sub,
       intermediate_21.intermediate_20_t3_sub AS intermediate_21_intermediate_20_t3_sub,
       intermediate_21.intermediate_20_intermediate_19_intermediate_0_obj AS intermediate_21_intermediate_20_intermediate_19_intermediate_0_obj,
       intermediate_21.t8_sub AS intermediate_21_t8_sub,
       intermediate_21.intermediate_20_intermediate_19_intermediate_2_obj AS intermediate_21_intermediate_20_intermediate_19_intermediate_2_obj,
       intermediate_21.t8_obj AS intermediate_21_t8_obj,
       intermediate_21.intermediate_20_t3_obj AS intermediate_21_intermediate_20_t3_obj,
       intermediate_24.intermediate_23_intermediate_22_intermediate_9_sub AS intermediate_24_intermediate_23_intermediate_22_intermediate_9_sub,
       intermediate_24.intermediate_11_sub AS intermediate_24_intermediate_11_sub,
       intermediate_24.intermediate_23_intermediate_4_sub AS intermediate_24_intermediate_23_intermediate_4_sub,
       intermediate_24.intermediate_23_intermediate_22_intermediate_6_obj AS intermediate_24_intermediate_23_intermediate_22_intermediate_6_obj,
       intermediate_24.intermediate_23_intermediate_4_obj AS intermediate_24_intermediate_23_intermediate_4_obj,
       intermediate_24.intermediate_23_intermediate_22_intermediate_6_sub AS intermediate_24_intermediate_23_intermediate_22_intermediate_6_sub,
       intermediate_24.intermediate_23_intermediate_22_intermediate_9_obj AS intermediate_24_intermediate_23_intermediate_22_intermediate_9_obj,
       intermediate_24.intermediate_11_obj AS intermediate_24_intermediate_11_obj
FROM 
(SELECT intermediate_23.intermediate_22_intermediate_6_sub AS intermediate_23_intermediate_22_intermediate_6_sub,
       intermediate_23.intermediate_22_intermediate_6_obj AS intermediate_23_intermediate_22_intermediate_6_obj,
       intermediate_23.intermediate_22_intermediate_9_obj AS intermediate_23_intermediate_22_intermediate_9_obj,
       intermediate_23.intermediate_22_intermediate_9_sub AS intermediate_23_intermediate_22_intermediate_9_sub,
       intermediate_23.intermediate_4_sub AS intermediate_23_intermediate_4_sub,
       intermediate_23.intermediate_4_obj AS intermediate_23_intermediate_4_obj,
       intermediate_11.sub AS intermediate_11_sub,
       intermediate_11.obj AS intermediate_11_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.obj = c.val) AS intermediate_11
     JOIN 
(SELECT intermediate_4.sub AS intermediate_4_sub,
       intermediate_4.obj AS intermediate_4_obj,
       intermediate_22.intermediate_9_sub AS intermediate_22_intermediate_9_sub,
       intermediate_22.intermediate_6_sub AS intermediate_22_intermediate_6_sub,
       intermediate_22.intermediate_9_obj AS intermediate_22_intermediate_9_obj,
       intermediate_22.intermediate_6_obj AS intermediate_22_intermediate_6_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.sub = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_4
     JOIN 
(SELECT intermediate_9.sub AS intermediate_9_sub,
       intermediate_9.obj AS intermediate_9_obj,
       intermediate_6.sub AS intermediate_6_sub,
       intermediate_6.obj AS intermediate_6_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.sub = c.val) AS intermediate_9
     JOIN 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.obj = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_6
     ON forceorder(intermediate_9.obj = intermediate_6.sub)) AS intermediate_22 ON forceorder(intermediate_4.obj = intermediate_22.intermediate_9_sub AND intermediate_4.sub = intermediate_22.intermediate_6_obj)) AS intermediate_23 ON forceorder(intermediate_23.intermediate_22_intermediate_9_sub = intermediate_11.obj)) AS intermediate_24 JOIN 
(SELECT intermediate_20.intermediate_19_intermediate_2_sub AS intermediate_20_intermediate_19_intermediate_2_sub,
       intermediate_20.intermediate_19_intermediate_2_obj AS intermediate_20_intermediate_19_intermediate_2_obj,
       intermediate_20.intermediate_19_intermediate_0_sub AS intermediate_20_intermediate_19_intermediate_0_sub,
       intermediate_20.t3_sub AS intermediate_20_t3_sub,
       intermediate_20.t3_obj AS intermediate_20_t3_obj,
       intermediate_20.intermediate_19_intermediate_0_obj AS intermediate_20_intermediate_19_intermediate_0_obj,
       t8.sub AS t8_sub,
       t8.obj AS t8_obj
FROM gplus AS t8 JOIN 
(SELECT intermediate_19.intermediate_2_sub AS intermediate_19_intermediate_2_sub,
       intermediate_19.intermediate_2_obj AS intermediate_19_intermediate_2_obj,
       intermediate_19.intermediate_0_obj AS intermediate_19_intermediate_0_obj,
       intermediate_19.intermediate_0_sub AS intermediate_19_intermediate_0_sub,
       t3.sub AS t3_sub,
       t3.obj AS t3_obj
FROM gplus AS t3 JOIN 
(SELECT intermediate_0.sub AS intermediate_0_sub,
       intermediate_0.obj AS intermediate_0_obj,
       intermediate_2.sub AS intermediate_2_sub,
       intermediate_2.obj AS intermediate_2_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.sub = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_2
     JOIN 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.obj = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_0
     ON forceorder(intermediate_0.obj = intermediate_2.sub)) AS intermediate_19 ON forceorder(intermediate_19.intermediate_0_sub = t3.obj AND intermediate_19.intermediate_2_obj = t3.sub)) AS intermediate_20 ON forceorder(intermediate_20.intermediate_19_intermediate_2_sub = t8.obj)) AS intermediate_21 ON forceorder(intermediate_21.t8_sub = intermediate_24.intermediate_11_sub AND intermediate_21.intermediate_20_intermediate_19_intermediate_2_obj = intermediate_24.intermediate_23_intermediate_22_intermediate_9_obj)) AS intermediate_25
        ;EXPLAIN ANALYZE 
SELECT COUNT(*)
FROM 
(SELECT intermediate_32.intermediate_31_intermediate_30_intermediate_0_sub AS intermediate_32_intermediate_31_intermediate_30_intermediate_0_sub,
       intermediate_32.intermediate_31_intermediate_30_intermediate_2_obj AS intermediate_32_intermediate_31_intermediate_30_intermediate_2_obj,
       intermediate_32.intermediate_31_intermediate_30_intermediate_2_sub AS intermediate_32_intermediate_31_intermediate_30_intermediate_2_sub,
       intermediate_32.t8_sub AS intermediate_32_t8_sub,
       intermediate_32.t8_obj AS intermediate_32_t8_obj,
       intermediate_32.intermediate_31_intermediate_30_intermediate_0_obj AS intermediate_32_intermediate_31_intermediate_30_intermediate_0_obj,
       intermediate_32.intermediate_31_t3_sub AS intermediate_32_intermediate_31_t3_sub,
       intermediate_32.intermediate_31_t3_obj AS intermediate_32_intermediate_31_t3_obj,
       intermediate_35.intermediate_28_sub AS intermediate_35_intermediate_28_sub,
       intermediate_35.intermediate_34_intermediate_33_intermediate_26_sub AS intermediate_35_intermediate_34_intermediate_33_intermediate_26_sub,
       intermediate_35.intermediate_34_intermediate_33_intermediate_26_obj AS intermediate_35_intermediate_34_intermediate_33_intermediate_26_obj,
       intermediate_35.intermediate_34_intermediate_33_intermediate_7_sub AS intermediate_35_intermediate_34_intermediate_33_intermediate_7_sub,
       intermediate_35.intermediate_34_intermediate_33_intermediate_7_obj AS intermediate_35_intermediate_34_intermediate_33_intermediate_7_obj,
       intermediate_35.intermediate_34_intermediate_5_obj AS intermediate_35_intermediate_34_intermediate_5_obj,
       intermediate_35.intermediate_28_obj AS intermediate_35_intermediate_28_obj,
       intermediate_35.intermediate_34_intermediate_5_sub AS intermediate_35_intermediate_34_intermediate_5_sub
FROM 
(SELECT intermediate_34.intermediate_33_intermediate_7_obj AS intermediate_34_intermediate_33_intermediate_7_obj,
       intermediate_34.intermediate_5_obj AS intermediate_34_intermediate_5_obj,
       intermediate_34.intermediate_33_intermediate_26_obj AS intermediate_34_intermediate_33_intermediate_26_obj,
       intermediate_34.intermediate_33_intermediate_26_sub AS intermediate_34_intermediate_33_intermediate_26_sub,
       intermediate_34.intermediate_33_intermediate_7_sub AS intermediate_34_intermediate_33_intermediate_7_sub,
       intermediate_34.intermediate_5_sub AS intermediate_34_intermediate_5_sub,
       intermediate_28.sub AS intermediate_28_sub,
       intermediate_28.obj AS intermediate_28_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.obj = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_28
     JOIN 
(SELECT intermediate_5.sub AS intermediate_5_sub,
       intermediate_5.obj AS intermediate_5_obj,
       intermediate_33.intermediate_7_obj AS intermediate_33_intermediate_7_obj,
       intermediate_33.intermediate_26_sub AS intermediate_33_intermediate_26_sub,
       intermediate_33.intermediate_7_sub AS intermediate_33_intermediate_7_sub,
       intermediate_33.intermediate_26_obj AS intermediate_33_intermediate_26_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.sub = c.val) AS intermediate_5
     JOIN 
(SELECT intermediate_26.sub AS intermediate_26_sub,
       intermediate_26.obj AS intermediate_26_obj,
       intermediate_7.sub AS intermediate_7_sub,
       intermediate_7.obj AS intermediate_7_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.obj = c.val) AS intermediate_7
     JOIN 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.sub = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_26
     ON forceorder(intermediate_26.obj = intermediate_7.sub)) AS intermediate_33 ON forceorder(intermediate_5.obj = intermediate_33.intermediate_26_sub AND intermediate_5.sub = intermediate_33.intermediate_7_obj)) AS intermediate_34 ON forceorder(intermediate_34.intermediate_33_intermediate_26_sub = intermediate_28.obj)) AS intermediate_35 JOIN 
(SELECT intermediate_31.intermediate_30_intermediate_2_obj AS intermediate_31_intermediate_30_intermediate_2_obj,
       intermediate_31.intermediate_30_intermediate_0_obj AS intermediate_31_intermediate_30_intermediate_0_obj,
       intermediate_31.intermediate_30_intermediate_0_sub AS intermediate_31_intermediate_30_intermediate_0_sub,
       intermediate_31.intermediate_30_intermediate_2_sub AS intermediate_31_intermediate_30_intermediate_2_sub,
       intermediate_31.t3_sub AS intermediate_31_t3_sub,
       intermediate_31.t3_obj AS intermediate_31_t3_obj,
       t8.sub AS t8_sub,
       t8.obj AS t8_obj
FROM gplus AS t8 JOIN 
(SELECT intermediate_30.intermediate_2_sub AS intermediate_30_intermediate_2_sub,
       intermediate_30.intermediate_2_obj AS intermediate_30_intermediate_2_obj,
       intermediate_30.intermediate_0_obj AS intermediate_30_intermediate_0_obj,
       intermediate_30.intermediate_0_sub AS intermediate_30_intermediate_0_sub,
       t3.sub AS t3_sub,
       t3.obj AS t3_obj
FROM gplus AS t3 JOIN 
(SELECT intermediate_0.sub AS intermediate_0_sub,
       intermediate_0.obj AS intermediate_0_obj,
       intermediate_2.sub AS intermediate_2_sub,
       intermediate_2.obj AS intermediate_2_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.sub = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_2
     JOIN 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.obj = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_0
     ON forceorder(intermediate_0.obj = intermediate_2.sub)) AS intermediate_30 ON forceorder(intermediate_30.intermediate_0_sub = t3.obj AND intermediate_30.intermediate_2_obj = t3.sub)) AS intermediate_31 ON forceorder(intermediate_31.intermediate_30_intermediate_2_sub = t8.obj)) AS intermediate_32 ON forceorder(intermediate_32.t8_sub = intermediate_35.intermediate_28_sub AND intermediate_32.intermediate_31_intermediate_30_intermediate_2_obj = intermediate_35.intermediate_34_intermediate_33_intermediate_26_obj)) AS intermediate_36
        ;EXPLAIN ANALYZE 
SELECT COUNT(*)
FROM 
(SELECT intermediate_41.intermediate_40_intermediate_39_intermediate_7_sub AS intermediate_41_intermediate_40_intermediate_39_intermediate_7_sub,
       intermediate_41.intermediate_38_intermediate_37_intermediate_2_sub AS intermediate_41_intermediate_38_intermediate_37_intermediate_2_sub,
       intermediate_41.intermediate_40_intermediate_39_intermediate_27_sub AS intermediate_41_intermediate_40_intermediate_39_intermediate_27_sub,
       intermediate_41.intermediate_40_intermediate_5_sub AS intermediate_41_intermediate_40_intermediate_5_sub,
       intermediate_41.intermediate_38_intermediate_37_intermediate_0_sub AS intermediate_41_intermediate_38_intermediate_37_intermediate_0_sub,
       intermediate_41.intermediate_38_t3_sub AS intermediate_41_intermediate_38_t3_sub,
       intermediate_41.intermediate_38_t3_obj AS intermediate_41_intermediate_38_t3_obj,
       intermediate_41.intermediate_40_intermediate_39_intermediate_7_obj AS intermediate_41_intermediate_40_intermediate_39_intermediate_7_obj,
       intermediate_41.intermediate_38_intermediate_37_intermediate_0_obj AS intermediate_41_intermediate_38_intermediate_37_intermediate_0_obj,
       intermediate_41.intermediate_40_intermediate_39_intermediate_27_obj AS intermediate_41_intermediate_40_intermediate_39_intermediate_27_obj,
       intermediate_41.intermediate_38_intermediate_37_intermediate_2_obj AS intermediate_41_intermediate_38_intermediate_37_intermediate_2_obj,
       intermediate_41.intermediate_40_intermediate_5_obj AS intermediate_41_intermediate_40_intermediate_5_obj,
       intermediate_42.t8_obj AS intermediate_42_t8_obj,
       intermediate_42.intermediate_29_obj AS intermediate_42_intermediate_29_obj,
       intermediate_42.t8_sub AS intermediate_42_t8_sub,
       intermediate_42.intermediate_29_sub AS intermediate_42_intermediate_29_sub
FROM 
(SELECT t8.sub AS t8_sub,
       t8.obj AS t8_obj,
       intermediate_29.sub AS intermediate_29_sub,
       intermediate_29.obj AS intermediate_29_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.obj = c.val) AS intermediate_29
     JOIN gplus AS t8 ON forceorder(t8.sub = intermediate_29.sub)) AS intermediate_42 JOIN 
(SELECT intermediate_38.intermediate_37_intermediate_0_obj AS intermediate_38_intermediate_37_intermediate_0_obj,
       intermediate_38.intermediate_37_intermediate_2_sub AS intermediate_38_intermediate_37_intermediate_2_sub,
       intermediate_38.intermediate_37_intermediate_0_sub AS intermediate_38_intermediate_37_intermediate_0_sub,
       intermediate_38.t3_sub AS intermediate_38_t3_sub,
       intermediate_38.intermediate_37_intermediate_2_obj AS intermediate_38_intermediate_37_intermediate_2_obj,
       intermediate_38.t3_obj AS intermediate_38_t3_obj,
       intermediate_40.intermediate_39_intermediate_7_obj AS intermediate_40_intermediate_39_intermediate_7_obj,
       intermediate_40.intermediate_5_obj AS intermediate_40_intermediate_5_obj,
       intermediate_40.intermediate_39_intermediate_7_sub AS intermediate_40_intermediate_39_intermediate_7_sub,
       intermediate_40.intermediate_39_intermediate_27_obj AS intermediate_40_intermediate_39_intermediate_27_obj,
       intermediate_40.intermediate_39_intermediate_27_sub AS intermediate_40_intermediate_39_intermediate_27_sub,
       intermediate_40.intermediate_5_sub AS intermediate_40_intermediate_5_sub
FROM 
(SELECT intermediate_5.sub AS intermediate_5_sub,
       intermediate_5.obj AS intermediate_5_obj,
       intermediate_39.intermediate_27_sub AS intermediate_39_intermediate_27_sub,
       intermediate_39.intermediate_7_sub AS intermediate_39_intermediate_7_sub,
       intermediate_39.intermediate_7_obj AS intermediate_39_intermediate_7_obj,
       intermediate_39.intermediate_27_obj AS intermediate_39_intermediate_27_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.sub = c.val) AS intermediate_5
     JOIN 
(SELECT intermediate_27.sub AS intermediate_27_sub,
       intermediate_27.obj AS intermediate_27_obj,
       intermediate_7.sub AS intermediate_7_sub,
       intermediate_7.obj AS intermediate_7_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.obj = c.val) AS intermediate_7
     JOIN 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.sub = c.val) AS intermediate_27
     ON forceorder(intermediate_27.obj = intermediate_7.sub)) AS intermediate_39 ON forceorder(intermediate_5.obj = intermediate_39.intermediate_27_sub AND intermediate_5.sub = intermediate_39.intermediate_7_obj)) AS intermediate_40 JOIN 
(SELECT intermediate_37.intermediate_2_sub AS intermediate_37_intermediate_2_sub,
       intermediate_37.intermediate_2_obj AS intermediate_37_intermediate_2_obj,
       intermediate_37.intermediate_0_obj AS intermediate_37_intermediate_0_obj,
       intermediate_37.intermediate_0_sub AS intermediate_37_intermediate_0_sub,
       t3.sub AS t3_sub,
       t3.obj AS t3_obj
FROM gplus AS t3 JOIN 
(SELECT intermediate_0.sub AS intermediate_0_sub,
       intermediate_0.obj AS intermediate_0_obj,
       intermediate_2.sub AS intermediate_2_sub,
       intermediate_2.obj AS intermediate_2_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.sub = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_2
     JOIN 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.obj = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_0
     ON forceorder(intermediate_0.obj = intermediate_2.sub)) AS intermediate_37 ON forceorder(intermediate_37.intermediate_0_sub = t3.obj AND intermediate_37.intermediate_2_obj = t3.sub)) AS intermediate_38 ON forceorder(intermediate_38.intermediate_37_intermediate_2_obj = intermediate_40.intermediate_39_intermediate_27_obj)) AS intermediate_41 ON forceorder(intermediate_41.intermediate_38_intermediate_37_intermediate_2_sub = intermediate_42.t8_obj AND intermediate_41.intermediate_40_intermediate_39_intermediate_27_sub = intermediate_42.intermediate_29_obj)) AS intermediate_43
        ;EXPLAIN ANALYZE 
SELECT COUNT(*)
FROM 
(SELECT intermediate_54.intermediate_53_intermediate_52_intermediate_3_obj AS intermediate_54_intermediate_53_intermediate_52_intermediate_3_obj,
       intermediate_54.intermediate_53_intermediate_52_t3_sub AS intermediate_54_intermediate_53_intermediate_52_t3_sub,
       intermediate_54.t8_sub AS intermediate_54_t8_sub,
       intermediate_54.intermediate_53_intermediate_1_obj AS intermediate_54_intermediate_53_intermediate_1_obj,
       intermediate_54.intermediate_53_intermediate_1_sub AS intermediate_54_intermediate_53_intermediate_1_sub,
       intermediate_54.intermediate_53_intermediate_52_t3_obj AS intermediate_54_intermediate_53_intermediate_52_t3_obj,
       intermediate_54.t8_obj AS intermediate_54_t8_obj,
       intermediate_54.intermediate_53_intermediate_52_intermediate_3_sub AS intermediate_54_intermediate_53_intermediate_52_intermediate_3_sub,
       intermediate_57.intermediate_56_intermediate_55_intermediate_46_sub AS intermediate_57_intermediate_56_intermediate_55_intermediate_46_sub,
       intermediate_57.intermediate_50_sub AS intermediate_57_intermediate_50_sub,
       intermediate_57.intermediate_56_intermediate_55_intermediate_44_obj AS intermediate_57_intermediate_56_intermediate_55_intermediate_44_obj,
       intermediate_57.intermediate_56_intermediate_55_intermediate_44_sub AS intermediate_57_intermediate_56_intermediate_55_intermediate_44_sub,
       intermediate_57.intermediate_56_intermediate_55_intermediate_46_obj AS intermediate_57_intermediate_56_intermediate_55_intermediate_46_obj,
       intermediate_57.intermediate_56_intermediate_48_sub AS intermediate_57_intermediate_56_intermediate_48_sub,
       intermediate_57.intermediate_56_intermediate_48_obj AS intermediate_57_intermediate_56_intermediate_48_obj,
       intermediate_57.intermediate_50_obj AS intermediate_57_intermediate_50_obj
FROM 
(SELECT intermediate_53.intermediate_52_intermediate_3_obj AS intermediate_53_intermediate_52_intermediate_3_obj,
       intermediate_53.intermediate_1_sub AS intermediate_53_intermediate_1_sub,
       intermediate_53.intermediate_1_obj AS intermediate_53_intermediate_1_obj,
       intermediate_53.intermediate_52_intermediate_3_sub AS intermediate_53_intermediate_52_intermediate_3_sub,
       intermediate_53.intermediate_52_t3_obj AS intermediate_53_intermediate_52_t3_obj,
       intermediate_53.intermediate_52_t3_sub AS intermediate_53_intermediate_52_t3_sub,
       t8.sub AS t8_sub,
       t8.obj AS t8_obj
FROM gplus AS t8 JOIN 
(SELECT intermediate_1.sub AS intermediate_1_sub,
       intermediate_1.obj AS intermediate_1_obj,
       intermediate_52.intermediate_3_sub AS intermediate_52_intermediate_3_sub,
       intermediate_52.t3_sub AS intermediate_52_t3_sub,
       intermediate_52.t3_obj AS intermediate_52_t3_obj,
       intermediate_52.intermediate_3_obj AS intermediate_52_intermediate_3_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.obj = c.val) AS intermediate_1
     JOIN 
(SELECT intermediate_3.sub AS intermediate_3_sub,
       intermediate_3.obj AS intermediate_3_obj,
       t3.sub AS t3_sub,
       t3.obj AS t3_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.sub = c.val) AS intermediate_3
     JOIN gplus AS t3 ON forceorder(intermediate_3.obj = t3.sub)) AS intermediate_52 ON forceorder(intermediate_1.obj = intermediate_52.intermediate_3_sub AND intermediate_1.sub = intermediate_52.t3_obj)) AS intermediate_53 ON forceorder(intermediate_53.intermediate_52_intermediate_3_sub = t8.obj)) AS intermediate_54 JOIN 
(SELECT intermediate_56.intermediate_55_intermediate_44_sub AS intermediate_56_intermediate_55_intermediate_44_sub,
       intermediate_56.intermediate_55_intermediate_46_sub AS intermediate_56_intermediate_55_intermediate_46_sub,
       intermediate_56.intermediate_48_sub AS intermediate_56_intermediate_48_sub,
       intermediate_56.intermediate_48_obj AS intermediate_56_intermediate_48_obj,
       intermediate_56.intermediate_55_intermediate_44_obj AS intermediate_56_intermediate_55_intermediate_44_obj,
       intermediate_56.intermediate_55_intermediate_46_obj AS intermediate_56_intermediate_55_intermediate_46_obj,
       intermediate_50.sub AS intermediate_50_sub,
       intermediate_50.obj AS intermediate_50_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.obj = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_50
     JOIN 
(SELECT intermediate_48.sub AS intermediate_48_sub,
       intermediate_48.obj AS intermediate_48_obj,
       intermediate_55.intermediate_44_sub AS intermediate_55_intermediate_44_sub,
       intermediate_55.intermediate_46_obj AS intermediate_55_intermediate_46_obj,
       intermediate_55.intermediate_46_sub AS intermediate_55_intermediate_46_sub,
       intermediate_55.intermediate_44_obj AS intermediate_55_intermediate_44_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.sub = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_48
     JOIN 
(SELECT intermediate_44.sub AS intermediate_44_sub,
       intermediate_44.obj AS intermediate_44_obj,
       intermediate_46.sub AS intermediate_46_sub,
       intermediate_46.obj AS intermediate_46_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.obj = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_46
     JOIN 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.sub = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_44
     ON forceorder(intermediate_44.sub = intermediate_46.obj)) AS intermediate_55 ON forceorder(intermediate_48.sub = intermediate_55.intermediate_44_obj AND intermediate_48.obj = intermediate_55.intermediate_46_sub)) AS intermediate_56 ON forceorder(intermediate_56.intermediate_48_sub = intermediate_50.obj)) AS intermediate_57 ON forceorder(intermediate_54.t8_sub = intermediate_57.intermediate_50_sub AND intermediate_54.intermediate_53_intermediate_52_intermediate_3_obj = intermediate_57.intermediate_56_intermediate_48_obj)) AS intermediate_58
        ;EXPLAIN ANALYZE 
SELECT COUNT(*)
FROM 
(SELECT intermediate_60.intermediate_59_intermediate_46_sub AS intermediate_60_intermediate_59_intermediate_46_sub,
       intermediate_60.intermediate_59_intermediate_46_obj AS intermediate_60_intermediate_59_intermediate_46_obj,
       intermediate_60.intermediate_59_intermediate_49_obj AS intermediate_60_intermediate_59_intermediate_49_obj,
       intermediate_60.intermediate_44_obj AS intermediate_60_intermediate_44_obj,
       intermediate_60.intermediate_44_sub AS intermediate_60_intermediate_44_sub,
       intermediate_60.intermediate_59_intermediate_49_sub AS intermediate_60_intermediate_59_intermediate_49_sub,
       intermediate_64.intermediate_61_intermediate_51_sub AS intermediate_64_intermediate_61_intermediate_51_sub,
       intermediate_64.intermediate_63_intermediate_62_t3_obj AS intermediate_64_intermediate_63_intermediate_62_t3_obj,
       intermediate_64.intermediate_63_intermediate_1_sub AS intermediate_64_intermediate_63_intermediate_1_sub,
       intermediate_64.intermediate_63_intermediate_62_intermediate_3_sub AS intermediate_64_intermediate_63_intermediate_62_intermediate_3_sub,
       intermediate_64.intermediate_61_t8_sub AS intermediate_64_intermediate_61_t8_sub,
       intermediate_64.intermediate_63_intermediate_62_intermediate_3_obj AS intermediate_64_intermediate_63_intermediate_62_intermediate_3_obj,
       intermediate_64.intermediate_63_intermediate_1_obj AS intermediate_64_intermediate_63_intermediate_1_obj,
       intermediate_64.intermediate_61_intermediate_51_obj AS intermediate_64_intermediate_61_intermediate_51_obj,
       intermediate_64.intermediate_61_t8_obj AS intermediate_64_intermediate_61_t8_obj,
       intermediate_64.intermediate_63_intermediate_62_t3_sub AS intermediate_64_intermediate_63_intermediate_62_t3_sub
FROM 
(SELECT intermediate_44.sub AS intermediate_44_sub,
       intermediate_44.obj AS intermediate_44_obj,
       intermediate_59.intermediate_46_obj AS intermediate_59_intermediate_46_obj,
       intermediate_59.intermediate_49_sub AS intermediate_59_intermediate_49_sub,
       intermediate_59.intermediate_46_sub AS intermediate_59_intermediate_46_sub,
       intermediate_59.intermediate_49_obj AS intermediate_59_intermediate_49_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.sub = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_44
     JOIN 
(SELECT intermediate_49.sub AS intermediate_49_sub,
       intermediate_49.obj AS intermediate_49_obj,
       intermediate_46.sub AS intermediate_46_sub,
       intermediate_46.obj AS intermediate_46_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.sub = c.val) AS intermediate_49
     JOIN 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.obj = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_46
     ON forceorder(intermediate_49.obj = intermediate_46.sub)) AS intermediate_59 ON forceorder(intermediate_44.obj = intermediate_59.intermediate_49_sub AND intermediate_44.sub = intermediate_59.intermediate_46_obj)) AS intermediate_60 JOIN 
(SELECT intermediate_61.t8_obj AS intermediate_61_t8_obj,
       intermediate_61.intermediate_51_obj AS intermediate_61_intermediate_51_obj,
       intermediate_61.t8_sub AS intermediate_61_t8_sub,
       intermediate_61.intermediate_51_sub AS intermediate_61_intermediate_51_sub,
       intermediate_63.intermediate_62_t3_obj AS intermediate_63_intermediate_62_t3_obj,
       intermediate_63.intermediate_62_intermediate_3_obj AS intermediate_63_intermediate_62_intermediate_3_obj,
       intermediate_63.intermediate_1_sub AS intermediate_63_intermediate_1_sub,
       intermediate_63.intermediate_1_obj AS intermediate_63_intermediate_1_obj,
       intermediate_63.intermediate_62_t3_sub AS intermediate_63_intermediate_62_t3_sub,
       intermediate_63.intermediate_62_intermediate_3_sub AS intermediate_63_intermediate_62_intermediate_3_sub
FROM 
(SELECT intermediate_1.sub AS intermediate_1_sub,
       intermediate_1.obj AS intermediate_1_obj,
       intermediate_62.intermediate_3_sub AS intermediate_62_intermediate_3_sub,
       intermediate_62.t3_sub AS intermediate_62_t3_sub,
       intermediate_62.t3_obj AS intermediate_62_t3_obj,
       intermediate_62.intermediate_3_obj AS intermediate_62_intermediate_3_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.obj = c.val) AS intermediate_1
     JOIN 
(SELECT intermediate_3.sub AS intermediate_3_sub,
       intermediate_3.obj AS intermediate_3_obj,
       t3.sub AS t3_sub,
       t3.obj AS t3_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.sub = c.val) AS intermediate_3
     JOIN gplus AS t3 ON forceorder(intermediate_3.obj = t3.sub)) AS intermediate_62 ON forceorder(intermediate_1.obj = intermediate_62.intermediate_3_sub AND intermediate_1.sub = intermediate_62.t3_obj)) AS intermediate_63 JOIN 
(SELECT t8.sub AS t8_sub,
       t8.obj AS t8_obj,
       intermediate_51.sub AS intermediate_51_sub,
       intermediate_51.obj AS intermediate_51_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.obj = c.val) AS intermediate_51
     JOIN gplus AS t8 ON forceorder(t8.sub = intermediate_51.sub)) AS intermediate_61 ON forceorder(intermediate_61.t8_obj = intermediate_63.intermediate_62_intermediate_3_sub)) AS intermediate_64 ON forceorder(intermediate_60.intermediate_59_intermediate_49_sub = intermediate_64.intermediate_61_intermediate_51_obj AND intermediate_60.intermediate_59_intermediate_49_obj = intermediate_64.intermediate_63_intermediate_62_intermediate_3_obj)) AS intermediate_65
        ;EXPLAIN ANALYZE 
SELECT COUNT(*)
FROM 
(SELECT intermediate_72.intermediate_71_intermediate_1_obj AS intermediate_72_intermediate_71_intermediate_1_obj,
       intermediate_72.intermediate_71_intermediate_70_intermediate_3_obj AS intermediate_72_intermediate_71_intermediate_70_intermediate_3_obj,
       intermediate_72.t8_sub AS intermediate_72_t8_sub,
       intermediate_72.intermediate_71_intermediate_70_intermediate_3_sub AS intermediate_72_intermediate_71_intermediate_70_intermediate_3_sub,
       intermediate_72.intermediate_71_intermediate_70_t3_sub AS intermediate_72_intermediate_71_intermediate_70_t3_sub,
       intermediate_72.t8_obj AS intermediate_72_t8_obj,
       intermediate_72.intermediate_71_intermediate_70_t3_obj AS intermediate_72_intermediate_71_intermediate_70_t3_obj,
       intermediate_72.intermediate_71_intermediate_1_sub AS intermediate_72_intermediate_71_intermediate_1_sub,
       intermediate_75.intermediate_68_sub AS intermediate_75_intermediate_68_sub,
       intermediate_75.intermediate_68_obj AS intermediate_75_intermediate_68_obj,
       intermediate_75.intermediate_74_intermediate_45_obj AS intermediate_75_intermediate_74_intermediate_45_obj,
       intermediate_75.intermediate_74_intermediate_73_intermediate_66_sub AS intermediate_75_intermediate_74_intermediate_73_intermediate_66_sub,
       intermediate_75.intermediate_74_intermediate_45_sub AS intermediate_75_intermediate_74_intermediate_45_sub,
       intermediate_75.intermediate_74_intermediate_73_intermediate_66_obj AS intermediate_75_intermediate_74_intermediate_73_intermediate_66_obj,
       intermediate_75.intermediate_74_intermediate_73_intermediate_47_sub AS intermediate_75_intermediate_74_intermediate_73_intermediate_47_sub,
       intermediate_75.intermediate_74_intermediate_73_intermediate_47_obj AS intermediate_75_intermediate_74_intermediate_73_intermediate_47_obj
FROM 
(SELECT intermediate_74.intermediate_73_intermediate_66_sub AS intermediate_74_intermediate_73_intermediate_66_sub,
       intermediate_74.intermediate_45_obj AS intermediate_74_intermediate_45_obj,
       intermediate_74.intermediate_45_sub AS intermediate_74_intermediate_45_sub,
       intermediate_74.intermediate_73_intermediate_66_obj AS intermediate_74_intermediate_73_intermediate_66_obj,
       intermediate_74.intermediate_73_intermediate_47_sub AS intermediate_74_intermediate_73_intermediate_47_sub,
       intermediate_74.intermediate_73_intermediate_47_obj AS intermediate_74_intermediate_73_intermediate_47_obj,
       intermediate_68.sub AS intermediate_68_sub,
       intermediate_68.obj AS intermediate_68_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.obj = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_68
     JOIN 
(SELECT intermediate_45.sub AS intermediate_45_sub,
       intermediate_45.obj AS intermediate_45_obj,
       intermediate_73.intermediate_47_obj AS intermediate_73_intermediate_47_obj,
       intermediate_73.intermediate_47_sub AS intermediate_73_intermediate_47_sub,
       intermediate_73.intermediate_66_sub AS intermediate_73_intermediate_66_sub,
       intermediate_73.intermediate_66_obj AS intermediate_73_intermediate_66_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.sub = c.val) AS intermediate_45
     JOIN 
(SELECT intermediate_66.sub AS intermediate_66_sub,
       intermediate_66.obj AS intermediate_66_obj,
       intermediate_47.sub AS intermediate_47_sub,
       intermediate_47.obj AS intermediate_47_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.obj = c.val) AS intermediate_47
     JOIN 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.sub = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_66
     ON forceorder(intermediate_66.obj = intermediate_47.sub)) AS intermediate_73 ON forceorder(intermediate_45.obj = intermediate_73.intermediate_66_sub AND intermediate_45.sub = intermediate_73.intermediate_47_obj)) AS intermediate_74 ON forceorder(intermediate_74.intermediate_73_intermediate_66_sub = intermediate_68.obj)) AS intermediate_75 JOIN 
(SELECT intermediate_71.intermediate_1_sub AS intermediate_71_intermediate_1_sub,
       intermediate_71.intermediate_1_obj AS intermediate_71_intermediate_1_obj,
       intermediate_71.intermediate_70_intermediate_3_sub AS intermediate_71_intermediate_70_intermediate_3_sub,
       intermediate_71.intermediate_70_intermediate_3_obj AS intermediate_71_intermediate_70_intermediate_3_obj,
       intermediate_71.intermediate_70_t3_sub AS intermediate_71_intermediate_70_t3_sub,
       intermediate_71.intermediate_70_t3_obj AS intermediate_71_intermediate_70_t3_obj,
       t8.sub AS t8_sub,
       t8.obj AS t8_obj
FROM gplus AS t8 JOIN 
(SELECT intermediate_1.sub AS intermediate_1_sub,
       intermediate_1.obj AS intermediate_1_obj,
       intermediate_70.intermediate_3_sub AS intermediate_70_intermediate_3_sub,
       intermediate_70.t3_sub AS intermediate_70_t3_sub,
       intermediate_70.t3_obj AS intermediate_70_t3_obj,
       intermediate_70.intermediate_3_obj AS intermediate_70_intermediate_3_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.obj = c.val) AS intermediate_1
     JOIN 
(SELECT intermediate_3.sub AS intermediate_3_sub,
       intermediate_3.obj AS intermediate_3_obj,
       t3.sub AS t3_sub,
       t3.obj AS t3_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.sub = c.val) AS intermediate_3
     JOIN gplus AS t3 ON forceorder(intermediate_3.obj = t3.sub)) AS intermediate_70 ON forceorder(intermediate_1.obj = intermediate_70.intermediate_3_sub AND intermediate_1.sub = intermediate_70.t3_obj)) AS intermediate_71 ON forceorder(intermediate_71.intermediate_70_intermediate_3_sub = t8.obj)) AS intermediate_72 ON forceorder(intermediate_72.t8_sub = intermediate_75.intermediate_68_sub AND intermediate_72.intermediate_71_intermediate_70_intermediate_3_obj = intermediate_75.intermediate_74_intermediate_73_intermediate_66_obj)) AS intermediate_76
        ;EXPLAIN ANALYZE 
SELECT COUNT(*)
FROM 
(SELECT intermediate_81.intermediate_78_intermediate_1_obj AS intermediate_81_intermediate_78_intermediate_1_obj,
       intermediate_81.intermediate_80_intermediate_45_sub AS intermediate_81_intermediate_80_intermediate_45_sub,
       intermediate_81.intermediate_80_intermediate_45_obj AS intermediate_81_intermediate_80_intermediate_45_obj,
       intermediate_81.intermediate_80_intermediate_79_intermediate_47_obj AS intermediate_81_intermediate_80_intermediate_79_intermediate_47_obj,
       intermediate_81.intermediate_78_intermediate_77_intermediate_3_obj AS intermediate_81_intermediate_78_intermediate_77_intermediate_3_obj,
       intermediate_81.intermediate_78_intermediate_77_t3_sub AS intermediate_81_intermediate_78_intermediate_77_t3_sub,
       intermediate_81.intermediate_78_intermediate_77_t3_obj AS intermediate_81_intermediate_78_intermediate_77_t3_obj,
       intermediate_81.intermediate_80_intermediate_79_intermediate_47_sub AS intermediate_81_intermediate_80_intermediate_79_intermediate_47_sub,
       intermediate_81.intermediate_80_intermediate_79_intermediate_67_sub AS intermediate_81_intermediate_80_intermediate_79_intermediate_67_sub,
       intermediate_81.intermediate_78_intermediate_1_sub AS intermediate_81_intermediate_78_intermediate_1_sub,
       intermediate_81.intermediate_80_intermediate_79_intermediate_67_obj AS intermediate_81_intermediate_80_intermediate_79_intermediate_67_obj,
       intermediate_81.intermediate_78_intermediate_77_intermediate_3_sub AS intermediate_81_intermediate_78_intermediate_77_intermediate_3_sub,
       intermediate_82.t8_obj AS intermediate_82_t8_obj,
       intermediate_82.intermediate_69_sub AS intermediate_82_intermediate_69_sub,
       intermediate_82.t8_sub AS intermediate_82_t8_sub,
       intermediate_82.intermediate_69_obj AS intermediate_82_intermediate_69_obj
FROM 
(SELECT t8.sub AS t8_sub,
       t8.obj AS t8_obj,
       intermediate_69.sub AS intermediate_69_sub,
       intermediate_69.obj AS intermediate_69_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.obj = c.val) AS intermediate_69
     JOIN gplus AS t8 ON forceorder(t8.sub = intermediate_69.sub)) AS intermediate_82 JOIN 
(SELECT intermediate_78.intermediate_1_sub AS intermediate_78_intermediate_1_sub,
       intermediate_78.intermediate_1_obj AS intermediate_78_intermediate_1_obj,
       intermediate_78.intermediate_77_intermediate_3_obj AS intermediate_78_intermediate_77_intermediate_3_obj,
       intermediate_78.intermediate_77_t3_sub AS intermediate_78_intermediate_77_t3_sub,
       intermediate_78.intermediate_77_intermediate_3_sub AS intermediate_78_intermediate_77_intermediate_3_sub,
       intermediate_78.intermediate_77_t3_obj AS intermediate_78_intermediate_77_t3_obj,
       intermediate_80.intermediate_79_intermediate_67_obj AS intermediate_80_intermediate_79_intermediate_67_obj,
       intermediate_80.intermediate_79_intermediate_67_sub AS intermediate_80_intermediate_79_intermediate_67_sub,
       intermediate_80.intermediate_79_intermediate_47_sub AS intermediate_80_intermediate_79_intermediate_47_sub,
       intermediate_80.intermediate_45_sub AS intermediate_80_intermediate_45_sub,
       intermediate_80.intermediate_79_intermediate_47_obj AS intermediate_80_intermediate_79_intermediate_47_obj,
       intermediate_80.intermediate_45_obj AS intermediate_80_intermediate_45_obj
FROM 
(SELECT intermediate_45.sub AS intermediate_45_sub,
       intermediate_45.obj AS intermediate_45_obj,
       intermediate_79.intermediate_67_sub AS intermediate_79_intermediate_67_sub,
       intermediate_79.intermediate_47_sub AS intermediate_79_intermediate_47_sub,
       intermediate_79.intermediate_67_obj AS intermediate_79_intermediate_67_obj,
       intermediate_79.intermediate_47_obj AS intermediate_79_intermediate_47_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.sub = c.val) AS intermediate_45
     JOIN 
(SELECT intermediate_67.sub AS intermediate_67_sub,
       intermediate_67.obj AS intermediate_67_obj,
       intermediate_47.sub AS intermediate_47_sub,
       intermediate_47.obj AS intermediate_47_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.obj = c.val) AS intermediate_47
     JOIN 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.sub = c.val) AS intermediate_67
     ON forceorder(intermediate_67.obj = intermediate_47.sub)) AS intermediate_79 ON forceorder(intermediate_45.obj = intermediate_79.intermediate_67_sub AND intermediate_45.sub = intermediate_79.intermediate_47_obj)) AS intermediate_80 JOIN 
(SELECT intermediate_1.sub AS intermediate_1_sub,
       intermediate_1.obj AS intermediate_1_obj,
       intermediate_77.intermediate_3_sub AS intermediate_77_intermediate_3_sub,
       intermediate_77.t3_sub AS intermediate_77_t3_sub,
       intermediate_77.t3_obj AS intermediate_77_t3_obj,
       intermediate_77.intermediate_3_obj AS intermediate_77_intermediate_3_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.obj = c.val) AS intermediate_1
     JOIN 
(SELECT intermediate_3.sub AS intermediate_3_sub,
       intermediate_3.obj AS intermediate_3_obj,
       t3.sub AS t3_sub,
       t3.obj AS t3_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.sub = c.val) AS intermediate_3
     JOIN gplus AS t3 ON forceorder(intermediate_3.obj = t3.sub)) AS intermediate_77 ON forceorder(intermediate_1.obj = intermediate_77.intermediate_3_sub AND intermediate_1.sub = intermediate_77.t3_obj)) AS intermediate_78 ON forceorder(intermediate_78.intermediate_77_intermediate_3_obj = intermediate_80.intermediate_79_intermediate_67_obj)) AS intermediate_81 ON forceorder(intermediate_81.intermediate_78_intermediate_77_intermediate_3_sub = intermediate_82.t8_obj AND intermediate_81.intermediate_80_intermediate_79_intermediate_67_sub = intermediate_82.intermediate_69_obj)) AS intermediate_83
        ;