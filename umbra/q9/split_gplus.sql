
\o -
\set timeout 5400000
\echo running q9 gplus

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
(SELECT intermediate_14.intermediate_12_intermediate_4_obj AS intermediate_14_intermediate_12_intermediate_4_obj,
       intermediate_14.intermediate_13_intermediate_10_obj AS intermediate_14_intermediate_13_intermediate_10_obj,
       intermediate_14.intermediate_12_intermediate_6_sub AS intermediate_14_intermediate_12_intermediate_6_sub,
       intermediate_14.intermediate_13_intermediate_10_sub AS intermediate_14_intermediate_13_intermediate_10_sub,
       intermediate_14.intermediate_13_intermediate_8_sub AS intermediate_14_intermediate_13_intermediate_8_sub,
       intermediate_14.intermediate_12_intermediate_6_obj AS intermediate_14_intermediate_12_intermediate_6_obj,
       intermediate_14.intermediate_13_intermediate_8_obj AS intermediate_14_intermediate_13_intermediate_8_obj,
       intermediate_14.intermediate_12_intermediate_4_sub AS intermediate_14_intermediate_12_intermediate_4_sub,
       intermediate_16.intermediate_15_intermediate_0_obj AS intermediate_16_intermediate_15_intermediate_0_obj,
       intermediate_16.t6_sub AS intermediate_16_t6_sub,
       intermediate_16.t6_obj AS intermediate_16_t6_obj,
       intermediate_16.intermediate_15_intermediate_2_obj AS intermediate_16_intermediate_15_intermediate_2_obj,
       intermediate_16.intermediate_15_intermediate_0_sub AS intermediate_16_intermediate_15_intermediate_0_sub,
       intermediate_16.intermediate_15_intermediate_2_sub AS intermediate_16_intermediate_15_intermediate_2_sub
FROM 
(SELECT t6.sub AS t6_sub,
       t6.obj AS t6_obj,
       intermediate_15.intermediate_0_sub AS intermediate_15_intermediate_0_sub,
       intermediate_15.intermediate_2_obj AS intermediate_15_intermediate_2_obj,
       intermediate_15.intermediate_2_sub AS intermediate_15_intermediate_2_sub,
       intermediate_15.intermediate_0_obj AS intermediate_15_intermediate_0_obj
FROM gplus AS t6 JOIN 
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
     ON forceorder(intermediate_0.obj = intermediate_2.sub)) AS intermediate_15 ON forceorder(t6.sub = intermediate_15.intermediate_0_sub AND t6.obj = intermediate_15.intermediate_2_obj)) AS intermediate_16 JOIN 
(SELECT intermediate_12.intermediate_6_sub AS intermediate_12_intermediate_6_sub,
       intermediate_12.intermediate_6_obj AS intermediate_12_intermediate_6_obj,
       intermediate_12.intermediate_4_sub AS intermediate_12_intermediate_4_sub,
       intermediate_12.intermediate_4_obj AS intermediate_12_intermediate_4_obj,
       intermediate_13.intermediate_10_sub AS intermediate_13_intermediate_10_sub,
       intermediate_13.intermediate_8_obj AS intermediate_13_intermediate_8_obj,
       intermediate_13.intermediate_10_obj AS intermediate_13_intermediate_10_obj,
       intermediate_13.intermediate_8_sub AS intermediate_13_intermediate_8_sub
FROM 
(SELECT intermediate_8.sub AS intermediate_8_sub,
       intermediate_8.obj AS intermediate_8_obj,
       intermediate_10.sub AS intermediate_10_sub,
       intermediate_10.obj AS intermediate_10_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.sub = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_10
     JOIN 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.obj = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_8
     ON forceorder(intermediate_8.obj = intermediate_10.sub)) AS intermediate_13 JOIN 
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
     ON forceorder(intermediate_4.obj = intermediate_6.sub)) AS intermediate_12 ON forceorder(intermediate_12.intermediate_4_sub = intermediate_13.intermediate_8_sub AND intermediate_12.intermediate_6_obj = intermediate_13.intermediate_10_obj)) AS intermediate_14 ON forceorder(intermediate_14.intermediate_12_intermediate_6_obj = intermediate_16.intermediate_15_intermediate_0_sub AND intermediate_14.intermediate_13_intermediate_10_obj = intermediate_16.t6_sub)) AS intermediate_17
        ;EXPLAIN ANALYZE 
SELECT COUNT(*)
FROM 
(SELECT intermediate_20.intermediate_19_intermediate_11_sub AS intermediate_20_intermediate_19_intermediate_11_sub,
       intermediate_20.intermediate_18_intermediate_9_sub AS intermediate_20_intermediate_18_intermediate_9_sub,
       intermediate_20.intermediate_19_intermediate_6_sub AS intermediate_20_intermediate_19_intermediate_6_sub,
       intermediate_20.intermediate_19_intermediate_6_obj AS intermediate_20_intermediate_19_intermediate_6_obj,
       intermediate_20.intermediate_18_intermediate_4_sub AS intermediate_20_intermediate_18_intermediate_4_sub,
       intermediate_20.intermediate_18_intermediate_9_obj AS intermediate_20_intermediate_18_intermediate_9_obj,
       intermediate_20.intermediate_19_intermediate_11_obj AS intermediate_20_intermediate_19_intermediate_11_obj,
       intermediate_20.intermediate_18_intermediate_4_obj AS intermediate_20_intermediate_18_intermediate_4_obj,
       intermediate_22.t6_sub AS intermediate_22_t6_sub,
       intermediate_22.intermediate_21_intermediate_2_obj AS intermediate_22_intermediate_21_intermediate_2_obj,
       intermediate_22.intermediate_21_intermediate_2_sub AS intermediate_22_intermediate_21_intermediate_2_sub,
       intermediate_22.intermediate_21_intermediate_0_sub AS intermediate_22_intermediate_21_intermediate_0_sub,
       intermediate_22.t6_obj AS intermediate_22_t6_obj,
       intermediate_22.intermediate_21_intermediate_0_obj AS intermediate_22_intermediate_21_intermediate_0_obj
FROM 
(SELECT t6.sub AS t6_sub,
       t6.obj AS t6_obj,
       intermediate_21.intermediate_0_sub AS intermediate_21_intermediate_0_sub,
       intermediate_21.intermediate_2_obj AS intermediate_21_intermediate_2_obj,
       intermediate_21.intermediate_2_sub AS intermediate_21_intermediate_2_sub,
       intermediate_21.intermediate_0_obj AS intermediate_21_intermediate_0_obj
FROM gplus AS t6 JOIN 
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
     ON forceorder(intermediate_0.obj = intermediate_2.sub)) AS intermediate_21 ON forceorder(t6.sub = intermediate_21.intermediate_0_sub AND t6.obj = intermediate_21.intermediate_2_obj)) AS intermediate_22 JOIN 
(SELECT intermediate_18.intermediate_9_obj AS intermediate_18_intermediate_9_obj,
       intermediate_18.intermediate_9_sub AS intermediate_18_intermediate_9_sub,
       intermediate_18.intermediate_4_sub AS intermediate_18_intermediate_4_sub,
       intermediate_18.intermediate_4_obj AS intermediate_18_intermediate_4_obj,
       intermediate_19.intermediate_6_sub AS intermediate_19_intermediate_6_sub,
       intermediate_19.intermediate_11_sub AS intermediate_19_intermediate_11_sub,
       intermediate_19.intermediate_6_obj AS intermediate_19_intermediate_6_obj,
       intermediate_19.intermediate_11_obj AS intermediate_19_intermediate_11_obj
FROM 
(SELECT intermediate_4.sub AS intermediate_4_sub,
       intermediate_4.obj AS intermediate_4_obj,
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
    LEFT JOIN sub_obj c ON t.obj = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_4
     ON forceorder(intermediate_4.sub = intermediate_9.sub)) AS intermediate_18 JOIN 
(SELECT intermediate_6.sub AS intermediate_6_sub,
       intermediate_6.obj AS intermediate_6_obj,
       intermediate_11.sub AS intermediate_11_sub,
       intermediate_11.obj AS intermediate_11_obj
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
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_6
     ON forceorder(intermediate_6.obj = intermediate_11.obj)) AS intermediate_19 ON forceorder(intermediate_18.intermediate_4_obj = intermediate_19.intermediate_6_sub AND intermediate_18.intermediate_9_obj = intermediate_19.intermediate_11_sub)) AS intermediate_20 ON forceorder(intermediate_20.intermediate_19_intermediate_6_obj = intermediate_22.intermediate_21_intermediate_0_sub AND intermediate_20.intermediate_19_intermediate_11_obj = intermediate_22.t6_sub)) AS intermediate_23
        ;EXPLAIN ANALYZE 
SELECT COUNT(*)
FROM 
(SELECT intermediate_30.intermediate_29_intermediate_7_obj AS intermediate_30_intermediate_29_intermediate_7_obj,
       intermediate_30.intermediate_28_intermediate_5_sub AS intermediate_30_intermediate_28_intermediate_5_sub,
       intermediate_30.intermediate_29_intermediate_26_sub AS intermediate_30_intermediate_29_intermediate_26_sub,
       intermediate_30.intermediate_29_intermediate_26_obj AS intermediate_30_intermediate_29_intermediate_26_obj,
       intermediate_30.intermediate_28_intermediate_24_sub AS intermediate_30_intermediate_28_intermediate_24_sub,
       intermediate_30.intermediate_29_intermediate_7_sub AS intermediate_30_intermediate_29_intermediate_7_sub,
       intermediate_30.intermediate_28_intermediate_24_obj AS intermediate_30_intermediate_28_intermediate_24_obj,
       intermediate_30.intermediate_28_intermediate_5_obj AS intermediate_30_intermediate_28_intermediate_5_obj,
       intermediate_32.t6_sub AS intermediate_32_t6_sub,
       intermediate_32.intermediate_31_intermediate_0_obj AS intermediate_32_intermediate_31_intermediate_0_obj,
       intermediate_32.t6_obj AS intermediate_32_t6_obj,
       intermediate_32.intermediate_31_intermediate_2_sub AS intermediate_32_intermediate_31_intermediate_2_sub,
       intermediate_32.intermediate_31_intermediate_0_sub AS intermediate_32_intermediate_31_intermediate_0_sub,
       intermediate_32.intermediate_31_intermediate_2_obj AS intermediate_32_intermediate_31_intermediate_2_obj
FROM 
(SELECT t6.sub AS t6_sub,
       t6.obj AS t6_obj,
       intermediate_31.intermediate_0_sub AS intermediate_31_intermediate_0_sub,
       intermediate_31.intermediate_2_obj AS intermediate_31_intermediate_2_obj,
       intermediate_31.intermediate_2_sub AS intermediate_31_intermediate_2_sub,
       intermediate_31.intermediate_0_obj AS intermediate_31_intermediate_0_obj
FROM gplus AS t6 JOIN 
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
     ON forceorder(intermediate_0.obj = intermediate_2.sub)) AS intermediate_31 ON forceorder(t6.sub = intermediate_31.intermediate_0_sub AND t6.obj = intermediate_31.intermediate_2_obj)) AS intermediate_32 JOIN 
(SELECT intermediate_28.intermediate_24_sub AS intermediate_28_intermediate_24_sub,
       intermediate_28.intermediate_5_obj AS intermediate_28_intermediate_5_obj,
       intermediate_28.intermediate_5_sub AS intermediate_28_intermediate_5_sub,
       intermediate_28.intermediate_24_obj AS intermediate_28_intermediate_24_obj,
       intermediate_29.intermediate_7_obj AS intermediate_29_intermediate_7_obj,
       intermediate_29.intermediate_7_sub AS intermediate_29_intermediate_7_sub,
       intermediate_29.intermediate_26_obj AS intermediate_29_intermediate_26_obj,
       intermediate_29.intermediate_26_sub AS intermediate_29_intermediate_26_sub
FROM 
(SELECT intermediate_5.sub AS intermediate_5_sub,
       intermediate_5.obj AS intermediate_5_obj,
       intermediate_24.sub AS intermediate_24_sub,
       intermediate_24.obj AS intermediate_24_obj
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
    LEFT JOIN sub_obj c ON t.obj = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_24
     ON forceorder(intermediate_5.sub = intermediate_24.sub)) AS intermediate_28 JOIN 
(SELECT intermediate_7.sub AS intermediate_7_sub,
       intermediate_7.obj AS intermediate_7_obj,
       intermediate_26.sub AS intermediate_26_sub,
       intermediate_26.obj AS intermediate_26_obj
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
    LEFT JOIN sub_obj c ON t.sub = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_26
     ON forceorder(intermediate_7.obj = intermediate_26.obj)) AS intermediate_29 ON forceorder(intermediate_28.intermediate_5_obj = intermediate_29.intermediate_7_sub AND intermediate_28.intermediate_24_obj = intermediate_29.intermediate_26_sub)) AS intermediate_30 ON forceorder(intermediate_30.intermediate_29_intermediate_7_obj = intermediate_32.intermediate_31_intermediate_0_sub AND intermediate_30.intermediate_29_intermediate_26_obj = intermediate_32.t6_sub)) AS intermediate_33
        ;EXPLAIN ANALYZE 
SELECT COUNT(*)
FROM 
(SELECT intermediate_36.intermediate_35_intermediate_27_obj AS intermediate_36_intermediate_35_intermediate_27_obj,
       intermediate_36.intermediate_35_intermediate_27_sub AS intermediate_36_intermediate_35_intermediate_27_sub,
       intermediate_36.intermediate_34_intermediate_25_obj AS intermediate_36_intermediate_34_intermediate_25_obj,
       intermediate_36.intermediate_35_intermediate_7_sub AS intermediate_36_intermediate_35_intermediate_7_sub,
       intermediate_36.intermediate_34_intermediate_25_sub AS intermediate_36_intermediate_34_intermediate_25_sub,
       intermediate_36.intermediate_34_intermediate_5_sub AS intermediate_36_intermediate_34_intermediate_5_sub,
       intermediate_36.intermediate_35_intermediate_7_obj AS intermediate_36_intermediate_35_intermediate_7_obj,
       intermediate_36.intermediate_34_intermediate_5_obj AS intermediate_36_intermediate_34_intermediate_5_obj,
       intermediate_38.intermediate_37_intermediate_2_obj AS intermediate_38_intermediate_37_intermediate_2_obj,
       intermediate_38.t6_sub AS intermediate_38_t6_sub,
       intermediate_38.intermediate_37_intermediate_2_sub AS intermediate_38_intermediate_37_intermediate_2_sub,
       intermediate_38.intermediate_37_intermediate_0_obj AS intermediate_38_intermediate_37_intermediate_0_obj,
       intermediate_38.t6_obj AS intermediate_38_t6_obj,
       intermediate_38.intermediate_37_intermediate_0_sub AS intermediate_38_intermediate_37_intermediate_0_sub
FROM 
(SELECT t6.sub AS t6_sub,
       t6.obj AS t6_obj,
       intermediate_37.intermediate_0_sub AS intermediate_37_intermediate_0_sub,
       intermediate_37.intermediate_2_obj AS intermediate_37_intermediate_2_obj,
       intermediate_37.intermediate_2_sub AS intermediate_37_intermediate_2_sub,
       intermediate_37.intermediate_0_obj AS intermediate_37_intermediate_0_obj
FROM gplus AS t6 JOIN 
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
     ON forceorder(intermediate_0.obj = intermediate_2.sub)) AS intermediate_37 ON forceorder(t6.sub = intermediate_37.intermediate_0_sub AND t6.obj = intermediate_37.intermediate_2_obj)) AS intermediate_38 JOIN 
(SELECT intermediate_34.intermediate_25_sub AS intermediate_34_intermediate_25_sub,
       intermediate_34.intermediate_5_obj AS intermediate_34_intermediate_5_obj,
       intermediate_34.intermediate_5_sub AS intermediate_34_intermediate_5_sub,
       intermediate_34.intermediate_25_obj AS intermediate_34_intermediate_25_obj,
       intermediate_35.intermediate_27_sub AS intermediate_35_intermediate_27_sub,
       intermediate_35.intermediate_7_obj AS intermediate_35_intermediate_7_obj,
       intermediate_35.intermediate_7_sub AS intermediate_35_intermediate_7_sub,
       intermediate_35.intermediate_27_obj AS intermediate_35_intermediate_27_obj
FROM 
(SELECT intermediate_5.sub AS intermediate_5_sub,
       intermediate_5.obj AS intermediate_5_obj,
       intermediate_25.sub AS intermediate_25_sub,
       intermediate_25.obj AS intermediate_25_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.obj = c.val) AS intermediate_25
     JOIN 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.obj = c.val) AS intermediate_5
     ON forceorder(intermediate_5.sub = intermediate_25.sub)) AS intermediate_34 JOIN 
(SELECT intermediate_7.sub AS intermediate_7_sub,
       intermediate_7.obj AS intermediate_7_obj,
       intermediate_27.sub AS intermediate_27_sub,
       intermediate_27.obj AS intermediate_27_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.sub = c.val) AS intermediate_27
     JOIN 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.sub = c.val) AS intermediate_7
     ON forceorder(intermediate_7.obj = intermediate_27.obj)) AS intermediate_35 ON forceorder(intermediate_34.intermediate_5_obj = intermediate_35.intermediate_7_sub AND intermediate_34.intermediate_25_obj = intermediate_35.intermediate_27_sub)) AS intermediate_36 ON forceorder(intermediate_36.intermediate_35_intermediate_7_obj = intermediate_38.intermediate_37_intermediate_0_sub AND intermediate_36.intermediate_35_intermediate_27_obj = intermediate_38.t6_sub)) AS intermediate_39
        ;EXPLAIN ANALYZE 
SELECT COUNT(*)
FROM 
(SELECT intermediate_50.intermediate_48_intermediate_42_obj AS intermediate_50_intermediate_48_intermediate_42_obj,
       intermediate_50.intermediate_49_intermediate_44_sub AS intermediate_50_intermediate_49_intermediate_44_sub,
       intermediate_50.intermediate_49_intermediate_46_sub AS intermediate_50_intermediate_49_intermediate_46_sub,
       intermediate_50.intermediate_48_intermediate_40_obj AS intermediate_50_intermediate_48_intermediate_40_obj,
       intermediate_50.intermediate_48_intermediate_40_sub AS intermediate_50_intermediate_48_intermediate_40_sub,
       intermediate_50.intermediate_49_intermediate_44_obj AS intermediate_50_intermediate_49_intermediate_44_obj,
       intermediate_50.intermediate_49_intermediate_46_obj AS intermediate_50_intermediate_49_intermediate_46_obj,
       intermediate_50.intermediate_48_intermediate_42_sub AS intermediate_50_intermediate_48_intermediate_42_sub,
       intermediate_52.intermediate_3_sub AS intermediate_52_intermediate_3_sub,
       intermediate_52.intermediate_3_obj AS intermediate_52_intermediate_3_obj,
       intermediate_52.intermediate_51_intermediate_1_obj AS intermediate_52_intermediate_51_intermediate_1_obj,
       intermediate_52.intermediate_51_t6_sub AS intermediate_52_intermediate_51_t6_sub,
       intermediate_52.intermediate_51_t6_obj AS intermediate_52_intermediate_51_t6_obj,
       intermediate_52.intermediate_51_intermediate_1_sub AS intermediate_52_intermediate_51_intermediate_1_sub
FROM 
(SELECT intermediate_51.t6_obj AS intermediate_51_t6_obj,
       intermediate_51.intermediate_1_obj AS intermediate_51_intermediate_1_obj,
       intermediate_51.t6_sub AS intermediate_51_t6_sub,
       intermediate_51.intermediate_1_sub AS intermediate_51_intermediate_1_sub,
       intermediate_3.sub AS intermediate_3_sub,
       intermediate_3.obj AS intermediate_3_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.sub = c.val) AS intermediate_3
     JOIN 
(SELECT intermediate_1.sub AS intermediate_1_sub,
       intermediate_1.obj AS intermediate_1_obj,
       t6.sub AS t6_sub,
       t6.obj AS t6_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.obj = c.val) AS intermediate_1
     JOIN gplus AS t6 ON forceorder(intermediate_1.sub = t6.sub)) AS intermediate_51 ON forceorder(intermediate_51.intermediate_1_obj = intermediate_3.sub AND intermediate_51.t6_obj = intermediate_3.obj)) AS intermediate_52 JOIN 
(SELECT intermediate_48.intermediate_42_obj AS intermediate_48_intermediate_42_obj,
       intermediate_48.intermediate_40_obj AS intermediate_48_intermediate_40_obj,
       intermediate_48.intermediate_40_sub AS intermediate_48_intermediate_40_sub,
       intermediate_48.intermediate_42_sub AS intermediate_48_intermediate_42_sub,
       intermediate_49.intermediate_44_sub AS intermediate_49_intermediate_44_sub,
       intermediate_49.intermediate_46_sub AS intermediate_49_intermediate_46_sub,
       intermediate_49.intermediate_46_obj AS intermediate_49_intermediate_46_obj,
       intermediate_49.intermediate_44_obj AS intermediate_49_intermediate_44_obj
FROM 
(SELECT intermediate_44.sub AS intermediate_44_sub,
       intermediate_44.obj AS intermediate_44_obj,
       intermediate_46.sub AS intermediate_46_sub,
       intermediate_46.obj AS intermediate_46_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.sub = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_46
     JOIN 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.obj = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_44
     ON forceorder(intermediate_44.obj = intermediate_46.sub)) AS intermediate_49 JOIN 
(SELECT intermediate_40.sub AS intermediate_40_sub,
       intermediate_40.obj AS intermediate_40_obj,
       intermediate_42.sub AS intermediate_42_sub,
       intermediate_42.obj AS intermediate_42_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.sub = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_42
     JOIN 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.obj = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_40
     ON forceorder(intermediate_40.obj = intermediate_42.sub)) AS intermediate_48 ON forceorder(intermediate_48.intermediate_40_sub = intermediate_49.intermediate_44_sub AND intermediate_48.intermediate_42_obj = intermediate_49.intermediate_46_obj)) AS intermediate_50 ON forceorder(intermediate_50.intermediate_48_intermediate_42_obj = intermediate_52.intermediate_51_intermediate_1_sub AND intermediate_50.intermediate_49_intermediate_46_obj = intermediate_52.intermediate_51_t6_sub)) AS intermediate_53
        ;EXPLAIN ANALYZE 
SELECT COUNT(*)
FROM 
(SELECT intermediate_56.intermediate_54_intermediate_45_sub AS intermediate_56_intermediate_54_intermediate_45_sub,
       intermediate_56.intermediate_55_intermediate_42_sub AS intermediate_56_intermediate_55_intermediate_42_sub,
       intermediate_56.intermediate_55_intermediate_47_sub AS intermediate_56_intermediate_55_intermediate_47_sub,
       intermediate_56.intermediate_54_intermediate_40_obj AS intermediate_56_intermediate_54_intermediate_40_obj,
       intermediate_56.intermediate_54_intermediate_40_sub AS intermediate_56_intermediate_54_intermediate_40_sub,
       intermediate_56.intermediate_55_intermediate_47_obj AS intermediate_56_intermediate_55_intermediate_47_obj,
       intermediate_56.intermediate_55_intermediate_42_obj AS intermediate_56_intermediate_55_intermediate_42_obj,
       intermediate_56.intermediate_54_intermediate_45_obj AS intermediate_56_intermediate_54_intermediate_45_obj,
       intermediate_58.intermediate_3_sub AS intermediate_58_intermediate_3_sub,
       intermediate_58.intermediate_57_t6_sub AS intermediate_58_intermediate_57_t6_sub,
       intermediate_58.intermediate_57_t6_obj AS intermediate_58_intermediate_57_t6_obj,
       intermediate_58.intermediate_3_obj AS intermediate_58_intermediate_3_obj,
       intermediate_58.intermediate_57_intermediate_1_obj AS intermediate_58_intermediate_57_intermediate_1_obj,
       intermediate_58.intermediate_57_intermediate_1_sub AS intermediate_58_intermediate_57_intermediate_1_sub
FROM 
(SELECT intermediate_57.t6_obj AS intermediate_57_t6_obj,
       intermediate_57.intermediate_1_obj AS intermediate_57_intermediate_1_obj,
       intermediate_57.t6_sub AS intermediate_57_t6_sub,
       intermediate_57.intermediate_1_sub AS intermediate_57_intermediate_1_sub,
       intermediate_3.sub AS intermediate_3_sub,
       intermediate_3.obj AS intermediate_3_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.sub = c.val) AS intermediate_3
     JOIN 
(SELECT intermediate_1.sub AS intermediate_1_sub,
       intermediate_1.obj AS intermediate_1_obj,
       t6.sub AS t6_sub,
       t6.obj AS t6_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.obj = c.val) AS intermediate_1
     JOIN gplus AS t6 ON forceorder(intermediate_1.sub = t6.sub)) AS intermediate_57 ON forceorder(intermediate_57.intermediate_1_obj = intermediate_3.sub AND intermediate_57.t6_obj = intermediate_3.obj)) AS intermediate_58 JOIN 
(SELECT intermediate_54.intermediate_40_obj AS intermediate_54_intermediate_40_obj,
       intermediate_54.intermediate_40_sub AS intermediate_54_intermediate_40_sub,
       intermediate_54.intermediate_45_obj AS intermediate_54_intermediate_45_obj,
       intermediate_54.intermediate_45_sub AS intermediate_54_intermediate_45_sub,
       intermediate_55.intermediate_42_obj AS intermediate_55_intermediate_42_obj,
       intermediate_55.intermediate_47_obj AS intermediate_55_intermediate_47_obj,
       intermediate_55.intermediate_47_sub AS intermediate_55_intermediate_47_sub,
       intermediate_55.intermediate_42_sub AS intermediate_55_intermediate_42_sub
FROM 
(SELECT intermediate_40.sub AS intermediate_40_sub,
       intermediate_40.obj AS intermediate_40_obj,
       intermediate_45.sub AS intermediate_45_sub,
       intermediate_45.obj AS intermediate_45_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.obj = c.val) AS intermediate_45
     JOIN 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.obj = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_40
     ON forceorder(intermediate_40.sub = intermediate_45.sub)) AS intermediate_54 JOIN 
(SELECT intermediate_42.sub AS intermediate_42_sub,
       intermediate_42.obj AS intermediate_42_obj,
       intermediate_47.sub AS intermediate_47_sub,
       intermediate_47.obj AS intermediate_47_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.sub = c.val) AS intermediate_47
     JOIN 
    (SELECT t.* 
    FROM gplus t
    LEFT JOIN sub_obj c ON t.sub = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_42
     ON forceorder(intermediate_42.obj = intermediate_47.obj)) AS intermediate_55 ON forceorder(intermediate_54.intermediate_40_obj = intermediate_55.intermediate_42_sub AND intermediate_54.intermediate_45_obj = intermediate_55.intermediate_47_sub)) AS intermediate_56 ON forceorder(intermediate_56.intermediate_55_intermediate_42_obj = intermediate_58.intermediate_57_intermediate_1_sub AND intermediate_56.intermediate_55_intermediate_47_obj = intermediate_58.intermediate_57_t6_sub)) AS intermediate_59
        ;EXPLAIN ANALYZE 
SELECT COUNT(*)
FROM 
(SELECT intermediate_66.intermediate_64_intermediate_41_sub AS intermediate_66_intermediate_64_intermediate_41_sub,
       intermediate_66.intermediate_64_intermediate_41_obj AS intermediate_66_intermediate_64_intermediate_41_obj,
       intermediate_66.intermediate_65_intermediate_62_obj AS intermediate_66_intermediate_65_intermediate_62_obj,
       intermediate_66.intermediate_65_intermediate_43_obj AS intermediate_66_intermediate_65_intermediate_43_obj,
       intermediate_66.intermediate_64_intermediate_60_sub AS intermediate_66_intermediate_64_intermediate_60_sub,
       intermediate_66.intermediate_65_intermediate_62_sub AS intermediate_66_intermediate_65_intermediate_62_sub,
       intermediate_66.intermediate_64_intermediate_60_obj AS intermediate_66_intermediate_64_intermediate_60_obj,
       intermediate_66.intermediate_65_intermediate_43_sub AS intermediate_66_intermediate_65_intermediate_43_sub,
       intermediate_68.intermediate_3_sub AS intermediate_68_intermediate_3_sub,
       intermediate_68.intermediate_67_intermediate_1_obj AS intermediate_68_intermediate_67_intermediate_1_obj,
       intermediate_68.intermediate_3_obj AS intermediate_68_intermediate_3_obj,
       intermediate_68.intermediate_67_t6_obj AS intermediate_68_intermediate_67_t6_obj,
       intermediate_68.intermediate_67_t6_sub AS intermediate_68_intermediate_67_t6_sub,
       intermediate_68.intermediate_67_intermediate_1_sub AS intermediate_68_intermediate_67_intermediate_1_sub
FROM 
(SELECT intermediate_67.t6_obj AS intermediate_67_t6_obj,
       intermediate_67.intermediate_1_obj AS intermediate_67_intermediate_1_obj,
       intermediate_67.t6_sub AS intermediate_67_t6_sub,
       intermediate_67.intermediate_1_sub AS intermediate_67_intermediate_1_sub,
       intermediate_3.sub AS intermediate_3_sub,
       intermediate_3.obj AS intermediate_3_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.sub = c.val) AS intermediate_3
     JOIN 
(SELECT intermediate_1.sub AS intermediate_1_sub,
       intermediate_1.obj AS intermediate_1_obj,
       t6.sub AS t6_sub,
       t6.obj AS t6_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.obj = c.val) AS intermediate_1
     JOIN gplus AS t6 ON forceorder(intermediate_1.sub = t6.sub)) AS intermediate_67 ON forceorder(intermediate_67.intermediate_1_obj = intermediate_3.sub AND intermediate_67.t6_obj = intermediate_3.obj)) AS intermediate_68 JOIN 
(SELECT intermediate_64.intermediate_41_sub AS intermediate_64_intermediate_41_sub,
       intermediate_64.intermediate_41_obj AS intermediate_64_intermediate_41_obj,
       intermediate_64.intermediate_60_obj AS intermediate_64_intermediate_60_obj,
       intermediate_64.intermediate_60_sub AS intermediate_64_intermediate_60_sub,
       intermediate_65.intermediate_62_sub AS intermediate_65_intermediate_62_sub,
       intermediate_65.intermediate_43_sub AS intermediate_65_intermediate_43_sub,
       intermediate_65.intermediate_43_obj AS intermediate_65_intermediate_43_obj,
       intermediate_65.intermediate_62_obj AS intermediate_65_intermediate_62_obj
FROM 
(SELECT intermediate_41.sub AS intermediate_41_sub,
       intermediate_41.obj AS intermediate_41_obj,
       intermediate_60.sub AS intermediate_60_sub,
       intermediate_60.obj AS intermediate_60_obj
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
    LEFT JOIN sub_obj c ON t.obj = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_60
     ON forceorder(intermediate_41.sub = intermediate_60.sub)) AS intermediate_64 JOIN 
(SELECT intermediate_43.sub AS intermediate_43_sub,
       intermediate_43.obj AS intermediate_43_obj,
       intermediate_62.sub AS intermediate_62_sub,
       intermediate_62.obj AS intermediate_62_obj
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
    LEFT JOIN sub_obj c ON t.sub = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_62
     ON forceorder(intermediate_43.obj = intermediate_62.obj)) AS intermediate_65 ON forceorder(intermediate_64.intermediate_41_obj = intermediate_65.intermediate_43_sub AND intermediate_64.intermediate_60_obj = intermediate_65.intermediate_62_sub)) AS intermediate_66 ON forceorder(intermediate_66.intermediate_65_intermediate_43_obj = intermediate_68.intermediate_67_intermediate_1_sub AND intermediate_66.intermediate_65_intermediate_62_obj = intermediate_68.intermediate_67_t6_sub)) AS intermediate_69
        ;EXPLAIN ANALYZE 
SELECT COUNT(*)
FROM 
(SELECT intermediate_70.intermediate_41_sub AS intermediate_70_intermediate_41_sub,
       intermediate_70.intermediate_41_obj AS intermediate_70_intermediate_41_obj,
       intermediate_70.intermediate_61_sub AS intermediate_70_intermediate_61_sub,
       intermediate_70.intermediate_61_obj AS intermediate_70_intermediate_61_obj,
       intermediate_74.intermediate_73_intermediate_3_obj AS intermediate_74_intermediate_73_intermediate_3_obj,
       intermediate_74.intermediate_72_intermediate_63_sub AS intermediate_74_intermediate_72_intermediate_63_sub,
       intermediate_74.intermediate_73_t6_sub AS intermediate_74_intermediate_73_t6_sub,
       intermediate_74.intermediate_73_intermediate_3_sub AS intermediate_74_intermediate_73_intermediate_3_sub,
       intermediate_74.intermediate_72_intermediate_71_intermediate_1_sub AS intermediate_74_intermediate_72_intermediate_71_intermediate_1_sub,
       intermediate_74.intermediate_72_intermediate_71_intermediate_43_sub AS intermediate_74_intermediate_72_intermediate_71_intermediate_43_sub,
       intermediate_74.intermediate_72_intermediate_71_intermediate_43_obj AS intermediate_74_intermediate_72_intermediate_71_intermediate_43_obj,
       intermediate_74.intermediate_73_t6_obj AS intermediate_74_intermediate_73_t6_obj,
       intermediate_74.intermediate_72_intermediate_63_obj AS intermediate_74_intermediate_72_intermediate_63_obj,
       intermediate_74.intermediate_72_intermediate_71_intermediate_1_obj AS intermediate_74_intermediate_72_intermediate_71_intermediate_1_obj
FROM 
(SELECT intermediate_41.sub AS intermediate_41_sub,
       intermediate_41.obj AS intermediate_41_obj,
       intermediate_61.sub AS intermediate_61_sub,
       intermediate_61.obj AS intermediate_61_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.obj = c.val) AS intermediate_61
     JOIN 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.obj = c.val) AS intermediate_41
     ON forceorder(intermediate_41.sub = intermediate_61.sub)) AS intermediate_70 JOIN 
(SELECT intermediate_72.intermediate_63_obj AS intermediate_72_intermediate_63_obj,
       intermediate_72.intermediate_71_intermediate_43_sub AS intermediate_72_intermediate_71_intermediate_43_sub,
       intermediate_72.intermediate_71_intermediate_1_sub AS intermediate_72_intermediate_71_intermediate_1_sub,
       intermediate_72.intermediate_71_intermediate_1_obj AS intermediate_72_intermediate_71_intermediate_1_obj,
       intermediate_72.intermediate_71_intermediate_43_obj AS intermediate_72_intermediate_71_intermediate_43_obj,
       intermediate_72.intermediate_63_sub AS intermediate_72_intermediate_63_sub,
       intermediate_73.t6_obj AS intermediate_73_t6_obj,
       intermediate_73.intermediate_3_obj AS intermediate_73_intermediate_3_obj,
       intermediate_73.intermediate_3_sub AS intermediate_73_intermediate_3_sub,
       intermediate_73.t6_sub AS intermediate_73_t6_sub
FROM 
(SELECT t6.sub AS t6_sub,
       t6.obj AS t6_obj,
       intermediate_3.sub AS intermediate_3_sub,
       intermediate_3.obj AS intermediate_3_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.sub = c.val) AS intermediate_3
     JOIN gplus AS t6 ON forceorder(t6.obj = intermediate_3.obj)) AS intermediate_73 JOIN 
(SELECT intermediate_63.sub AS intermediate_63_sub,
       intermediate_63.obj AS intermediate_63_obj,
       intermediate_71.intermediate_43_sub AS intermediate_71_intermediate_43_sub,
       intermediate_71.intermediate_43_obj AS intermediate_71_intermediate_43_obj,
       intermediate_71.intermediate_1_obj AS intermediate_71_intermediate_1_obj,
       intermediate_71.intermediate_1_sub AS intermediate_71_intermediate_1_sub
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.sub = c.val) AS intermediate_63
     JOIN 
(SELECT intermediate_43.sub AS intermediate_43_sub,
       intermediate_43.obj AS intermediate_43_obj,
       intermediate_1.sub AS intermediate_1_sub,
       intermediate_1.obj AS intermediate_1_obj
FROM 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.obj = c.val) AS intermediate_1
     JOIN 
    (SELECT t.* 
    FROM gplus t
    JOIN (
        SELECT s.*
        FROM sub_obj s
        WHERE s.cnt > 862) c
        ON t.sub = c.val) AS intermediate_43
     ON forceorder(intermediate_43.obj = intermediate_1.sub)) AS intermediate_71 ON forceorder(intermediate_63.obj = intermediate_71.intermediate_43_obj)) AS intermediate_72 ON forceorder(intermediate_72.intermediate_63_obj = intermediate_73.t6_sub AND intermediate_72.intermediate_71_intermediate_1_sub = intermediate_73.t6_sub AND intermediate_72.intermediate_71_intermediate_1_obj = intermediate_73.intermediate_3_sub)) AS intermediate_74 ON forceorder(intermediate_70.intermediate_41_obj = intermediate_74.intermediate_72_intermediate_71_intermediate_43_sub AND intermediate_70.intermediate_61_obj = intermediate_74.intermediate_72_intermediate_63_sub)) AS intermediate_75
        ;