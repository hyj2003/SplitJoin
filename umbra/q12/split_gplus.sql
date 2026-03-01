
\o -
\set timeout 5400000
\echo running q12 gplus

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
(SELECT intermediate_12.intermediate_6_sub AS intermediate_12_intermediate_6_sub,
       intermediate_12.intermediate_6_obj AS intermediate_12_intermediate_6_obj,
       intermediate_12.intermediate_4_sub AS intermediate_12_intermediate_4_sub,
       intermediate_12.intermediate_4_obj AS intermediate_12_intermediate_4_obj,
       intermediate_15.intermediate_14_intermediate_13_intermediate_0_sub AS intermediate_15_intermediate_14_intermediate_13_intermediate_0_sub,
       intermediate_15.intermediate_14_intermediate_13_intermediate_2_obj AS intermediate_15_intermediate_14_intermediate_13_intermediate_2_obj,
       intermediate_15.intermediate_14_intermediate_13_intermediate_0_obj AS intermediate_15_intermediate_14_intermediate_13_intermediate_0_obj,
       intermediate_15.intermediate_14_intermediate_10_sub AS intermediate_15_intermediate_14_intermediate_10_sub,
       intermediate_15.intermediate_8_obj AS intermediate_15_intermediate_8_obj,
       intermediate_15.intermediate_14_intermediate_13_intermediate_2_sub AS intermediate_15_intermediate_14_intermediate_13_intermediate_2_sub,
       intermediate_15.intermediate_14_intermediate_10_obj AS intermediate_15_intermediate_14_intermediate_10_obj,
       intermediate_15.intermediate_8_sub AS intermediate_15_intermediate_8_sub
FROM 
(SELECT intermediate_4.sub AS intermediate_4_sub,
       intermediate_4.obj AS intermediate_4_obj,
       intermediate_6.sub AS intermediate_6_sub,
       intermediate_6.obj AS intermediate_6_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.sub = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_6
     JOIN 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.obj = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_4
     ON forceorder(intermediate_4.obj = intermediate_6.sub)) AS intermediate_12 JOIN 
(SELECT intermediate_8.sub AS intermediate_8_sub,
       intermediate_8.obj AS intermediate_8_obj,
       intermediate_14.intermediate_13_intermediate_2_obj AS intermediate_14_intermediate_13_intermediate_2_obj,
       intermediate_14.intermediate_13_intermediate_0_obj AS intermediate_14_intermediate_13_intermediate_0_obj,
       intermediate_14.intermediate_13_intermediate_0_sub AS intermediate_14_intermediate_13_intermediate_0_sub,
       intermediate_14.intermediate_10_sub AS intermediate_14_intermediate_10_sub,
       intermediate_14.intermediate_13_intermediate_2_sub AS intermediate_14_intermediate_13_intermediate_2_sub,
       intermediate_14.intermediate_10_obj AS intermediate_14_intermediate_10_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.obj = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_8
     JOIN 
(SELECT intermediate_13.intermediate_0_obj AS intermediate_13_intermediate_0_obj,
       intermediate_13.intermediate_0_sub AS intermediate_13_intermediate_0_sub,
       intermediate_13.intermediate_2_sub AS intermediate_13_intermediate_2_sub,
       intermediate_13.intermediate_2_obj AS intermediate_13_intermediate_2_obj,
       intermediate_10.obj AS intermediate_10_obj,
       intermediate_10.sub AS intermediate_10_sub
FROM 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.sub = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_10
     JOIN 
(SELECT intermediate_0.obj AS intermediate_0_obj,
       intermediate_0.sub AS intermediate_0_sub,
       intermediate_2.sub AS intermediate_2_sub,
       intermediate_2.obj AS intermediate_2_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.obj = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_2
     JOIN 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.sub = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_0
     ON forceorder(intermediate_0.sub = intermediate_2.obj)) AS intermediate_13 ON forceorder(intermediate_13.intermediate_0_obj = intermediate_10.obj AND intermediate_13.intermediate_2_sub = intermediate_10.sub)) AS intermediate_14 ON forceorder(intermediate_8.obj = intermediate_14.intermediate_13_intermediate_2_sub AND intermediate_8.obj = intermediate_14.intermediate_10_sub)) AS intermediate_15 ON forceorder(intermediate_12.intermediate_4_sub = intermediate_15.intermediate_14_intermediate_13_intermediate_0_obj AND intermediate_12.intermediate_4_sub = intermediate_15.intermediate_14_intermediate_10_obj AND intermediate_12.intermediate_6_obj = intermediate_15.intermediate_8_sub)) AS intermediate_16
        ;EXPLAIN ANALYZE 
SELECT COUNT(*)
FROM 
(SELECT intermediate_18.intermediate_17_intermediate_9_sub AS intermediate_18_intermediate_17_intermediate_9_sub,
       intermediate_18.intermediate_4_sub AS intermediate_18_intermediate_4_sub,
       intermediate_18.intermediate_17_intermediate_6_sub AS intermediate_18_intermediate_17_intermediate_6_sub,
       intermediate_18.intermediate_17_intermediate_9_obj AS intermediate_18_intermediate_17_intermediate_9_obj,
       intermediate_18.intermediate_4_obj AS intermediate_18_intermediate_4_obj,
       intermediate_18.intermediate_17_intermediate_6_obj AS intermediate_18_intermediate_17_intermediate_6_obj,
       intermediate_20.intermediate_19_intermediate_0_sub AS intermediate_20_intermediate_19_intermediate_0_sub,
       intermediate_20.intermediate_19_intermediate_11_obj AS intermediate_20_intermediate_19_intermediate_11_obj,
       intermediate_20.intermediate_2_obj AS intermediate_20_intermediate_2_obj,
       intermediate_20.intermediate_19_intermediate_0_obj AS intermediate_20_intermediate_19_intermediate_0_obj,
       intermediate_20.intermediate_2_sub AS intermediate_20_intermediate_2_sub,
       intermediate_20.intermediate_19_intermediate_11_sub AS intermediate_20_intermediate_19_intermediate_11_sub
FROM 
(SELECT intermediate_2.sub AS intermediate_2_sub,
       intermediate_2.obj AS intermediate_2_obj,
       intermediate_19.intermediate_11_sub AS intermediate_19_intermediate_11_sub,
       intermediate_19.intermediate_0_obj AS intermediate_19_intermediate_0_obj,
       intermediate_19.intermediate_0_sub AS intermediate_19_intermediate_0_sub,
       intermediate_19.intermediate_11_obj AS intermediate_19_intermediate_11_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.obj = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_2
     JOIN 
(SELECT intermediate_0.obj AS intermediate_0_obj,
       intermediate_0.sub AS intermediate_0_sub,
       intermediate_11.obj AS intermediate_11_obj,
       intermediate_11.sub AS intermediate_11_sub
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.sub = c.val) AS intermediate_11
     JOIN 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.sub = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_0
     ON forceorder(intermediate_0.obj = intermediate_11.obj)) AS intermediate_19 ON forceorder(intermediate_2.obj = intermediate_19.intermediate_0_sub AND intermediate_2.sub = intermediate_19.intermediate_11_sub)) AS intermediate_20 JOIN 
(SELECT intermediate_4.sub AS intermediate_4_sub,
       intermediate_4.obj AS intermediate_4_obj,
       intermediate_17.intermediate_6_sub AS intermediate_17_intermediate_6_sub,
       intermediate_17.intermediate_6_obj AS intermediate_17_intermediate_6_obj,
       intermediate_17.intermediate_9_sub AS intermediate_17_intermediate_9_sub,
       intermediate_17.intermediate_9_obj AS intermediate_17_intermediate_9_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.obj = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_4
     JOIN 
(SELECT intermediate_6.sub AS intermediate_6_sub,
       intermediate_6.obj AS intermediate_6_obj,
       intermediate_9.sub AS intermediate_9_sub,
       intermediate_9.obj AS intermediate_9_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.obj = c.val) AS intermediate_9
     JOIN 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.sub = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_6
     ON forceorder(intermediate_6.obj = intermediate_9.sub)) AS intermediate_17 ON forceorder(intermediate_4.obj = intermediate_17.intermediate_6_sub)) AS intermediate_18 ON forceorder(intermediate_18.intermediate_4_sub = intermediate_20.intermediate_19_intermediate_0_obj AND intermediate_18.intermediate_4_sub = intermediate_20.intermediate_19_intermediate_11_obj AND intermediate_18.intermediate_17_intermediate_9_obj = intermediate_20.intermediate_19_intermediate_11_sub AND intermediate_18.intermediate_17_intermediate_9_obj = intermediate_20.intermediate_2_sub)) AS intermediate_21
        ;EXPLAIN ANALYZE 
SELECT COUNT(*)
FROM 
(SELECT intermediate_28.intermediate_26_intermediate_2_sub AS intermediate_28_intermediate_26_intermediate_2_sub,
       intermediate_28.intermediate_27_intermediate_5_obj AS intermediate_28_intermediate_27_intermediate_5_obj,
       intermediate_28.intermediate_26_intermediate_0_obj AS intermediate_28_intermediate_26_intermediate_0_obj,
       intermediate_28.intermediate_26_intermediate_2_obj AS intermediate_28_intermediate_26_intermediate_2_obj,
       intermediate_28.intermediate_27_intermediate_24_obj AS intermediate_28_intermediate_27_intermediate_24_obj,
       intermediate_28.intermediate_27_intermediate_24_sub AS intermediate_28_intermediate_27_intermediate_24_sub,
       intermediate_28.intermediate_27_intermediate_5_sub AS intermediate_28_intermediate_27_intermediate_5_sub,
       intermediate_28.intermediate_26_intermediate_0_sub AS intermediate_28_intermediate_26_intermediate_0_sub,
       intermediate_29.intermediate_7_sub AS intermediate_29_intermediate_7_sub,
       intermediate_29.intermediate_22_sub AS intermediate_29_intermediate_22_sub,
       intermediate_29.intermediate_22_obj AS intermediate_29_intermediate_22_obj,
       intermediate_29.intermediate_7_obj AS intermediate_29_intermediate_7_obj
FROM 
(SELECT intermediate_7.sub AS intermediate_7_sub,
       intermediate_7.obj AS intermediate_7_obj,
       intermediate_22.sub AS intermediate_22_sub,
       intermediate_22.obj AS intermediate_22_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.sub = c.val) AS intermediate_7
     JOIN 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.obj = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_22
     ON forceorder(intermediate_7.obj = intermediate_22.sub)) AS intermediate_29 JOIN 
(SELECT intermediate_26.intermediate_0_obj AS intermediate_26_intermediate_0_obj,
       intermediate_26.intermediate_0_sub AS intermediate_26_intermediate_0_sub,
       intermediate_26.intermediate_2_sub AS intermediate_26_intermediate_2_sub,
       intermediate_26.intermediate_2_obj AS intermediate_26_intermediate_2_obj,
       intermediate_27.intermediate_24_obj AS intermediate_27_intermediate_24_obj,
       intermediate_27.intermediate_5_obj AS intermediate_27_intermediate_5_obj,
       intermediate_27.intermediate_24_sub AS intermediate_27_intermediate_24_sub,
       intermediate_27.intermediate_5_sub AS intermediate_27_intermediate_5_sub
FROM 
(SELECT intermediate_5.sub AS intermediate_5_sub,
       intermediate_5.obj AS intermediate_5_obj,
       intermediate_24.obj AS intermediate_24_obj,
       intermediate_24.sub AS intermediate_24_sub
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.obj = c.val) AS intermediate_5
     JOIN 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.sub = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_24
     ON forceorder(intermediate_5.sub = intermediate_24.obj)) AS intermediate_27 JOIN 
(SELECT intermediate_0.obj AS intermediate_0_obj,
       intermediate_0.sub AS intermediate_0_sub,
       intermediate_2.sub AS intermediate_2_sub,
       intermediate_2.obj AS intermediate_2_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.obj = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_2
     JOIN 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.sub = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_0
     ON forceorder(intermediate_0.sub = intermediate_2.obj)) AS intermediate_26 ON forceorder(intermediate_26.intermediate_0_obj = intermediate_27.intermediate_5_sub AND intermediate_26.intermediate_0_obj = intermediate_27.intermediate_24_obj AND intermediate_26.intermediate_2_sub = intermediate_27.intermediate_24_sub)) AS intermediate_28 ON forceorder(intermediate_28.intermediate_27_intermediate_5_obj = intermediate_29.intermediate_7_sub AND intermediate_28.intermediate_26_intermediate_2_sub = intermediate_29.intermediate_22_obj AND intermediate_28.intermediate_27_intermediate_24_sub = intermediate_29.intermediate_22_obj)) AS intermediate_30
        ;EXPLAIN ANALYZE 
SELECT COUNT(*)
FROM 
(SELECT intermediate_31.intermediate_7_sub AS intermediate_31_intermediate_7_sub,
       intermediate_31.intermediate_23_sub AS intermediate_31_intermediate_23_sub,
       intermediate_31.intermediate_23_obj AS intermediate_31_intermediate_23_obj,
       intermediate_31.intermediate_7_obj AS intermediate_31_intermediate_7_obj,
       intermediate_34.intermediate_33_intermediate_2_sub AS intermediate_34_intermediate_33_intermediate_2_sub,
       intermediate_34.intermediate_33_intermediate_32_intermediate_0_obj AS intermediate_34_intermediate_33_intermediate_32_intermediate_0_obj,
       intermediate_34.intermediate_33_intermediate_2_obj AS intermediate_34_intermediate_33_intermediate_2_obj,
       intermediate_34.intermediate_5_sub AS intermediate_34_intermediate_5_sub,
       intermediate_34.intermediate_33_intermediate_32_intermediate_0_sub AS intermediate_34_intermediate_33_intermediate_32_intermediate_0_sub,
       intermediate_34.intermediate_5_obj AS intermediate_34_intermediate_5_obj,
       intermediate_34.intermediate_33_intermediate_32_intermediate_25_sub AS intermediate_34_intermediate_33_intermediate_32_intermediate_25_sub,
       intermediate_34.intermediate_33_intermediate_32_intermediate_25_obj AS intermediate_34_intermediate_33_intermediate_32_intermediate_25_obj
FROM 
(SELECT intermediate_7.sub AS intermediate_7_sub,
       intermediate_7.obj AS intermediate_7_obj,
       intermediate_23.sub AS intermediate_23_sub,
       intermediate_23.obj AS intermediate_23_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.obj = c.val) AS intermediate_23
     JOIN 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.sub = c.val) AS intermediate_7
     ON forceorder(intermediate_7.obj = intermediate_23.sub)) AS intermediate_31 JOIN 
(SELECT intermediate_5.sub AS intermediate_5_sub,
       intermediate_5.obj AS intermediate_5_obj,
       intermediate_33.intermediate_32_intermediate_0_sub AS intermediate_33_intermediate_32_intermediate_0_sub,
       intermediate_33.intermediate_32_intermediate_25_obj AS intermediate_33_intermediate_32_intermediate_25_obj,
       intermediate_33.intermediate_32_intermediate_0_obj AS intermediate_33_intermediate_32_intermediate_0_obj,
       intermediate_33.intermediate_2_obj AS intermediate_33_intermediate_2_obj,
       intermediate_33.intermediate_2_sub AS intermediate_33_intermediate_2_sub,
       intermediate_33.intermediate_32_intermediate_25_sub AS intermediate_33_intermediate_32_intermediate_25_sub
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.obj = c.val) AS intermediate_5
     JOIN 
(SELECT intermediate_2.sub AS intermediate_2_sub,
       intermediate_2.obj AS intermediate_2_obj,
       intermediate_32.intermediate_25_sub AS intermediate_32_intermediate_25_sub,
       intermediate_32.intermediate_0_obj AS intermediate_32_intermediate_0_obj,
       intermediate_32.intermediate_0_sub AS intermediate_32_intermediate_0_sub,
       intermediate_32.intermediate_25_obj AS intermediate_32_intermediate_25_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.obj = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_2
     JOIN 
(SELECT intermediate_0.obj AS intermediate_0_obj,
       intermediate_0.sub AS intermediate_0_sub,
       intermediate_25.obj AS intermediate_25_obj,
       intermediate_25.sub AS intermediate_25_sub
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.sub = c.val) AS intermediate_25
     JOIN 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.sub = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_0
     ON forceorder(intermediate_0.obj = intermediate_25.obj)) AS intermediate_32 ON forceorder(intermediate_2.obj = intermediate_32.intermediate_0_sub AND intermediate_2.sub = intermediate_32.intermediate_25_sub)) AS intermediate_33 ON forceorder(intermediate_5.sub = intermediate_33.intermediate_32_intermediate_0_obj AND intermediate_5.sub = intermediate_33.intermediate_32_intermediate_25_obj)) AS intermediate_34 ON forceorder(intermediate_31.intermediate_7_sub = intermediate_34.intermediate_5_obj AND intermediate_31.intermediate_23_obj = intermediate_34.intermediate_33_intermediate_2_sub AND intermediate_31.intermediate_23_obj = intermediate_34.intermediate_33_intermediate_32_intermediate_25_sub)) AS intermediate_35
        ;EXPLAIN ANALYZE 
SELECT COUNT(*)
FROM 
(SELECT intermediate_44.intermediate_36_sub AS intermediate_44_intermediate_36_sub,
       intermediate_44.intermediate_36_obj AS intermediate_44_intermediate_36_obj,
       intermediate_44.intermediate_38_obj AS intermediate_44_intermediate_38_obj,
       intermediate_44.intermediate_38_sub AS intermediate_44_intermediate_38_sub,
       intermediate_47.intermediate_46_intermediate_45_intermediate_3_sub AS intermediate_47_intermediate_46_intermediate_45_intermediate_3_sub,
       intermediate_47.intermediate_46_intermediate_45_intermediate_42_obj AS intermediate_47_intermediate_46_intermediate_45_intermediate_42_obj,
       intermediate_47.intermediate_46_intermediate_45_intermediate_3_obj AS intermediate_47_intermediate_46_intermediate_45_intermediate_3_obj,
       intermediate_47.intermediate_40_sub AS intermediate_47_intermediate_40_sub,
       intermediate_47.intermediate_46_intermediate_1_sub AS intermediate_47_intermediate_46_intermediate_1_sub,
       intermediate_47.intermediate_46_intermediate_1_obj AS intermediate_47_intermediate_46_intermediate_1_obj,
       intermediate_47.intermediate_46_intermediate_45_intermediate_42_sub AS intermediate_47_intermediate_46_intermediate_45_intermediate_42_sub,
       intermediate_47.intermediate_40_obj AS intermediate_47_intermediate_40_obj
FROM 
(SELECT intermediate_36.sub AS intermediate_36_sub,
       intermediate_36.obj AS intermediate_36_obj,
       intermediate_38.sub AS intermediate_38_sub,
       intermediate_38.obj AS intermediate_38_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.sub = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_38
     JOIN 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.obj = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_36
     ON forceorder(intermediate_36.obj = intermediate_38.sub)) AS intermediate_44 JOIN 
(SELECT intermediate_40.sub AS intermediate_40_sub,
       intermediate_40.obj AS intermediate_40_obj,
       intermediate_46.intermediate_1_obj AS intermediate_46_intermediate_1_obj,
       intermediate_46.intermediate_1_sub AS intermediate_46_intermediate_1_sub,
       intermediate_46.intermediate_45_intermediate_42_obj AS intermediate_46_intermediate_45_intermediate_42_obj,
       intermediate_46.intermediate_45_intermediate_3_sub AS intermediate_46_intermediate_45_intermediate_3_sub,
       intermediate_46.intermediate_45_intermediate_3_obj AS intermediate_46_intermediate_45_intermediate_3_obj,
       intermediate_46.intermediate_45_intermediate_42_sub AS intermediate_46_intermediate_45_intermediate_42_sub
FROM 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.obj = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_40
     JOIN 
(SELECT intermediate_1.obj AS intermediate_1_obj,
       intermediate_1.sub AS intermediate_1_sub,
       intermediate_45.intermediate_42_sub AS intermediate_45_intermediate_42_sub,
       intermediate_45.intermediate_42_obj AS intermediate_45_intermediate_42_obj,
       intermediate_45.intermediate_3_sub AS intermediate_45_intermediate_3_sub,
       intermediate_45.intermediate_3_obj AS intermediate_45_intermediate_3_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.sub = c.val) AS intermediate_1
     JOIN 
(SELECT intermediate_3.sub AS intermediate_3_sub,
       intermediate_3.obj AS intermediate_3_obj,
       intermediate_42.obj AS intermediate_42_obj,
       intermediate_42.sub AS intermediate_42_sub
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.obj = c.val) AS intermediate_3
     JOIN 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.sub = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_42
     ON forceorder(intermediate_3.sub = intermediate_42.sub)) AS intermediate_45 ON forceorder(intermediate_1.sub = intermediate_45.intermediate_3_obj AND intermediate_1.obj = intermediate_45.intermediate_42_obj)) AS intermediate_46 ON forceorder(intermediate_40.obj = intermediate_46.intermediate_45_intermediate_3_sub AND intermediate_40.obj = intermediate_46.intermediate_45_intermediate_42_sub)) AS intermediate_47 ON forceorder(intermediate_44.intermediate_36_sub = intermediate_47.intermediate_46_intermediate_1_obj AND intermediate_44.intermediate_36_sub = intermediate_47.intermediate_46_intermediate_45_intermediate_42_obj AND intermediate_44.intermediate_38_obj = intermediate_47.intermediate_40_sub)) AS intermediate_48
        ;EXPLAIN ANALYZE 
SELECT COUNT(*)
FROM 
(SELECT intermediate_49.intermediate_41_sub AS intermediate_49_intermediate_41_sub,
       intermediate_49.intermediate_41_obj AS intermediate_49_intermediate_41_obj,
       intermediate_49.intermediate_38_obj AS intermediate_49_intermediate_38_obj,
       intermediate_49.intermediate_38_sub AS intermediate_49_intermediate_38_sub,
       intermediate_52.intermediate_36_obj AS intermediate_52_intermediate_36_obj,
       intermediate_52.intermediate_51_intermediate_50_intermediate_43_obj AS intermediate_52_intermediate_51_intermediate_50_intermediate_43_obj,
       intermediate_52.intermediate_51_intermediate_1_obj AS intermediate_52_intermediate_51_intermediate_1_obj,
       intermediate_52.intermediate_51_intermediate_50_intermediate_43_sub AS intermediate_52_intermediate_51_intermediate_50_intermediate_43_sub,
       intermediate_52.intermediate_36_sub AS intermediate_52_intermediate_36_sub,
       intermediate_52.intermediate_51_intermediate_1_sub AS intermediate_52_intermediate_51_intermediate_1_sub,
       intermediate_52.intermediate_51_intermediate_50_intermediate_3_obj AS intermediate_52_intermediate_51_intermediate_50_intermediate_3_obj,
       intermediate_52.intermediate_51_intermediate_50_intermediate_3_sub AS intermediate_52_intermediate_51_intermediate_50_intermediate_3_sub
FROM 
(SELECT intermediate_38.sub AS intermediate_38_sub,
       intermediate_38.obj AS intermediate_38_obj,
       intermediate_41.sub AS intermediate_41_sub,
       intermediate_41.obj AS intermediate_41_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.obj = c.val) AS intermediate_41
     JOIN 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.sub = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_38
     ON forceorder(intermediate_38.obj = intermediate_41.sub)) AS intermediate_49 JOIN 
(SELECT intermediate_36.sub AS intermediate_36_sub,
       intermediate_36.obj AS intermediate_36_obj,
       intermediate_51.intermediate_1_obj AS intermediate_51_intermediate_1_obj,
       intermediate_51.intermediate_50_intermediate_3_sub AS intermediate_51_intermediate_50_intermediate_3_sub,
       intermediate_51.intermediate_50_intermediate_3_obj AS intermediate_51_intermediate_50_intermediate_3_obj,
       intermediate_51.intermediate_1_sub AS intermediate_51_intermediate_1_sub,
       intermediate_51.intermediate_50_intermediate_43_obj AS intermediate_51_intermediate_50_intermediate_43_obj,
       intermediate_51.intermediate_50_intermediate_43_sub AS intermediate_51_intermediate_50_intermediate_43_sub
FROM 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.obj = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_36
     JOIN 
(SELECT intermediate_1.obj AS intermediate_1_obj,
       intermediate_1.sub AS intermediate_1_sub,
       intermediate_50.intermediate_43_sub AS intermediate_50_intermediate_43_sub,
       intermediate_50.intermediate_43_obj AS intermediate_50_intermediate_43_obj,
       intermediate_50.intermediate_3_sub AS intermediate_50_intermediate_3_sub,
       intermediate_50.intermediate_3_obj AS intermediate_50_intermediate_3_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.sub = c.val) AS intermediate_1
     JOIN 
(SELECT intermediate_3.sub AS intermediate_3_sub,
       intermediate_3.obj AS intermediate_3_obj,
       intermediate_43.obj AS intermediate_43_obj,
       intermediate_43.sub AS intermediate_43_sub
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.sub = c.val) AS intermediate_43
     JOIN 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.obj = c.val) AS intermediate_3
     ON forceorder(intermediate_3.sub = intermediate_43.sub)) AS intermediate_50 ON forceorder(intermediate_1.sub = intermediate_50.intermediate_3_obj AND intermediate_1.obj = intermediate_50.intermediate_43_obj)) AS intermediate_51 ON forceorder(intermediate_36.sub = intermediate_51.intermediate_1_obj AND intermediate_36.sub = intermediate_51.intermediate_50_intermediate_43_obj)) AS intermediate_52 ON forceorder(intermediate_49.intermediate_38_sub = intermediate_52.intermediate_36_obj AND intermediate_49.intermediate_41_obj = intermediate_52.intermediate_51_intermediate_50_intermediate_3_sub AND intermediate_49.intermediate_41_obj = intermediate_52.intermediate_51_intermediate_50_intermediate_43_sub)) AS intermediate_53
        ;EXPLAIN ANALYZE 
SELECT COUNT(*)
FROM 
(SELECT intermediate_60.intermediate_58_intermediate_1_obj AS intermediate_60_intermediate_58_intermediate_1_obj,
       intermediate_60.intermediate_59_intermediate_56_obj AS intermediate_60_intermediate_59_intermediate_56_obj,
       intermediate_60.intermediate_59_intermediate_3_sub AS intermediate_60_intermediate_59_intermediate_3_sub,
       intermediate_60.intermediate_59_intermediate_56_sub AS intermediate_60_intermediate_59_intermediate_56_sub,
       intermediate_60.intermediate_58_intermediate_37_obj AS intermediate_60_intermediate_58_intermediate_37_obj,
       intermediate_60.intermediate_58_intermediate_37_sub AS intermediate_60_intermediate_58_intermediate_37_sub,
       intermediate_60.intermediate_58_intermediate_1_sub AS intermediate_60_intermediate_58_intermediate_1_sub,
       intermediate_60.intermediate_59_intermediate_3_obj AS intermediate_60_intermediate_59_intermediate_3_obj,
       intermediate_61.intermediate_54_sub AS intermediate_61_intermediate_54_sub,
       intermediate_61.intermediate_39_sub AS intermediate_61_intermediate_39_sub,
       intermediate_61.intermediate_54_obj AS intermediate_61_intermediate_54_obj,
       intermediate_61.intermediate_39_obj AS intermediate_61_intermediate_39_obj
FROM 
(SELECT intermediate_39.sub AS intermediate_39_sub,
       intermediate_39.obj AS intermediate_39_obj,
       intermediate_54.sub AS intermediate_54_sub,
       intermediate_54.obj AS intermediate_54_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.sub = c.val) AS intermediate_39
     JOIN 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.obj = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_54
     ON forceorder(intermediate_39.obj = intermediate_54.sub)) AS intermediate_61 JOIN 
(SELECT intermediate_58.intermediate_37_obj AS intermediate_58_intermediate_37_obj,
       intermediate_58.intermediate_1_obj AS intermediate_58_intermediate_1_obj,
       intermediate_58.intermediate_1_sub AS intermediate_58_intermediate_1_sub,
       intermediate_58.intermediate_37_sub AS intermediate_58_intermediate_37_sub,
       intermediate_59.intermediate_56_obj AS intermediate_59_intermediate_56_obj,
       intermediate_59.intermediate_56_sub AS intermediate_59_intermediate_56_sub,
       intermediate_59.intermediate_3_sub AS intermediate_59_intermediate_3_sub,
       intermediate_59.intermediate_3_obj AS intermediate_59_intermediate_3_obj
FROM 
(SELECT intermediate_37.sub AS intermediate_37_sub,
       intermediate_37.obj AS intermediate_37_obj,
       intermediate_1.obj AS intermediate_1_obj,
       intermediate_1.sub AS intermediate_1_sub
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.sub = c.val) AS intermediate_1
     JOIN 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.obj = c.val) AS intermediate_37
     ON forceorder(intermediate_37.sub = intermediate_1.obj)) AS intermediate_58 JOIN 
(SELECT intermediate_3.sub AS intermediate_3_sub,
       intermediate_3.obj AS intermediate_3_obj,
       intermediate_56.obj AS intermediate_56_obj,
       intermediate_56.sub AS intermediate_56_sub
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.obj = c.val) AS intermediate_3
     JOIN 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.sub = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_56
     ON forceorder(intermediate_3.sub = intermediate_56.sub)) AS intermediate_59 ON forceorder(intermediate_58.intermediate_1_sub = intermediate_59.intermediate_3_obj AND intermediate_58.intermediate_37_sub = intermediate_59.intermediate_56_obj AND intermediate_58.intermediate_1_obj = intermediate_59.intermediate_56_obj)) AS intermediate_60 ON forceorder(intermediate_60.intermediate_58_intermediate_37_obj = intermediate_61.intermediate_39_sub AND intermediate_60.intermediate_59_intermediate_3_sub = intermediate_61.intermediate_54_obj AND intermediate_60.intermediate_59_intermediate_56_sub = intermediate_61.intermediate_54_obj)) AS intermediate_62
        ;EXPLAIN ANALYZE 
SELECT COUNT(*)
FROM 
(SELECT intermediate_63.intermediate_55_sub AS intermediate_63_intermediate_55_sub,
       intermediate_63.intermediate_39_sub AS intermediate_63_intermediate_39_sub,
       intermediate_63.intermediate_39_obj AS intermediate_63_intermediate_39_obj,
       intermediate_63.intermediate_55_obj AS intermediate_63_intermediate_55_obj,
       intermediate_66.intermediate_65_intermediate_64_intermediate_3_obj AS intermediate_66_intermediate_65_intermediate_64_intermediate_3_obj,
       intermediate_66.intermediate_65_intermediate_64_intermediate_3_sub AS intermediate_66_intermediate_65_intermediate_64_intermediate_3_sub,
       intermediate_66.intermediate_37_sub AS intermediate_66_intermediate_37_sub,
       intermediate_66.intermediate_65_intermediate_64_intermediate_57_sub AS intermediate_66_intermediate_65_intermediate_64_intermediate_57_sub,
       intermediate_66.intermediate_65_intermediate_1_sub AS intermediate_66_intermediate_65_intermediate_1_sub,
       intermediate_66.intermediate_37_obj AS intermediate_66_intermediate_37_obj,
       intermediate_66.intermediate_65_intermediate_1_obj AS intermediate_66_intermediate_65_intermediate_1_obj,
       intermediate_66.intermediate_65_intermediate_64_intermediate_57_obj AS intermediate_66_intermediate_65_intermediate_64_intermediate_57_obj
FROM 
(SELECT intermediate_39.sub AS intermediate_39_sub,
       intermediate_39.obj AS intermediate_39_obj,
       intermediate_55.sub AS intermediate_55_sub,
       intermediate_55.obj AS intermediate_55_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.obj = c.val) AS intermediate_55
     JOIN 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.sub = c.val) AS intermediate_39
     ON forceorder(intermediate_39.obj = intermediate_55.sub)) AS intermediate_63 JOIN 
(SELECT intermediate_37.sub AS intermediate_37_sub,
       intermediate_37.obj AS intermediate_37_obj,
       intermediate_65.intermediate_1_obj AS intermediate_65_intermediate_1_obj,
       intermediate_65.intermediate_64_intermediate_57_obj AS intermediate_65_intermediate_64_intermediate_57_obj,
       intermediate_65.intermediate_64_intermediate_3_obj AS intermediate_65_intermediate_64_intermediate_3_obj,
       intermediate_65.intermediate_64_intermediate_57_sub AS intermediate_65_intermediate_64_intermediate_57_sub,
       intermediate_65.intermediate_1_sub AS intermediate_65_intermediate_1_sub,
       intermediate_65.intermediate_64_intermediate_3_sub AS intermediate_65_intermediate_64_intermediate_3_sub
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.obj = c.val) AS intermediate_37
     JOIN 
(SELECT intermediate_1.obj AS intermediate_1_obj,
       intermediate_1.sub AS intermediate_1_sub,
       intermediate_64.intermediate_57_sub AS intermediate_64_intermediate_57_sub,
       intermediate_64.intermediate_3_sub AS intermediate_64_intermediate_3_sub,
       intermediate_64.intermediate_57_obj AS intermediate_64_intermediate_57_obj,
       intermediate_64.intermediate_3_obj AS intermediate_64_intermediate_3_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.sub = c.val) AS intermediate_1
     JOIN 
(SELECT intermediate_3.sub AS intermediate_3_sub,
       intermediate_3.obj AS intermediate_3_obj,
       intermediate_57.obj AS intermediate_57_obj,
       intermediate_57.sub AS intermediate_57_sub
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.sub = c.val) AS intermediate_57
     JOIN 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.obj = c.val) AS intermediate_3
     ON forceorder(intermediate_3.sub = intermediate_57.sub)) AS intermediate_64 ON forceorder(intermediate_1.sub = intermediate_64.intermediate_3_obj AND intermediate_1.obj = intermediate_64.intermediate_57_obj)) AS intermediate_65 ON forceorder(intermediate_37.sub = intermediate_65.intermediate_1_obj AND intermediate_37.sub = intermediate_65.intermediate_64_intermediate_57_obj)) AS intermediate_66 ON forceorder(intermediate_63.intermediate_39_sub = intermediate_66.intermediate_37_obj AND intermediate_63.intermediate_55_obj = intermediate_66.intermediate_65_intermediate_64_intermediate_3_sub AND intermediate_63.intermediate_55_obj = intermediate_66.intermediate_65_intermediate_64_intermediate_57_sub)) AS intermediate_67
        ;