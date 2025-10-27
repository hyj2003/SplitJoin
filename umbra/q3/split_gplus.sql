
\o -
\set timeout 5400000
\echo running q3 gplus

DROP TABLE IF EXISTS group_obj, group_sub, sub_obj;
CREATE TABLE group_obj AS (SELECT obj AS val, COUNT(*) AS cnt FROM gplus GROUP BY obj ORDER BY cnt DESC LIMIT 100000);
CREATE TABLE group_sub AS (SELECT sub AS val, COUNT(*) AS cnt FROM gplus GROUP BY sub ORDER BY cnt DESC LIMIT 100000);
CREATE TABLE sub_obj AS (
SELECT group_obj.val AS val, LEAST(group_obj.cnt, group_sub.cnt) AS cnt
FROM group_obj JOIN group_sub
ON group_obj.val = group_sub.val
ORDER BY cnt DESC);
\echo join time:
\set repeat 4

SELECT COUNT(*)
FROM 
(SELECT intermediate_8.intermediate_1_sub AS intermediate_8_intermediate_1_sub,
       intermediate_8.intermediate_5_obj AS intermediate_8_intermediate_5_obj,
       intermediate_8.intermediate_5_sub AS intermediate_8_intermediate_5_sub,
       intermediate_8.intermediate_1_obj AS intermediate_8_intermediate_1_obj,
       intermediate_9.intermediate_3_obj AS intermediate_9_intermediate_3_obj,
       intermediate_9.intermediate_7_sub AS intermediate_9_intermediate_7_sub,
       intermediate_9.intermediate_3_sub AS intermediate_9_intermediate_3_sub,
       intermediate_9.intermediate_7_obj AS intermediate_9_intermediate_7_obj
FROM 
(SELECT intermediate_1.sub AS intermediate_1_sub,
       intermediate_1.obj AS intermediate_1_obj,
       intermediate_5.sub AS intermediate_5_sub,
       intermediate_5.obj AS intermediate_5_obj
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
JOIN (
    SELECT s.*
    FROM group_sub s
    WHERE s.cnt > 1526) c
    ON t.sub = c.val) AS intermediate_1
 ON forceorder(intermediate_1.obj = intermediate_5.sub)) AS intermediate_8 JOIN 
(SELECT intermediate_3.sub AS intermediate_3_sub,
       intermediate_3.obj AS intermediate_3_obj,
       intermediate_7.sub AS intermediate_7_sub,
       intermediate_7.obj AS intermediate_7_obj
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
JOIN (
    SELECT s.*
    FROM group_sub s
    WHERE s.cnt > 1526) c
    ON t.sub = c.val) AS intermediate_3
 ON forceorder(intermediate_3.obj = intermediate_7.obj)) AS intermediate_9 ON forceorder(intermediate_8.intermediate_1_sub = intermediate_9.intermediate_3_sub AND intermediate_8.intermediate_5_obj = intermediate_9.intermediate_7_sub)) AS intermediate_10
;
SELECT COUNT(*)
FROM 
(SELECT intermediate_11.intermediate_1_sub AS intermediate_11_intermediate_1_sub,
       intermediate_11.intermediate_4_sub AS intermediate_11_intermediate_4_sub,
       intermediate_11.intermediate_4_obj AS intermediate_11_intermediate_4_obj,
       intermediate_11.intermediate_1_obj AS intermediate_11_intermediate_1_obj,
       intermediate_12.intermediate_3_obj AS intermediate_12_intermediate_3_obj,
       intermediate_12.intermediate_6_obj AS intermediate_12_intermediate_6_obj,
       intermediate_12.intermediate_3_sub AS intermediate_12_intermediate_3_sub,
       intermediate_12.intermediate_6_sub AS intermediate_12_intermediate_6_sub
FROM 
(SELECT intermediate_1.sub AS intermediate_1_sub,
       intermediate_1.obj AS intermediate_1_obj,
       intermediate_4.sub AS intermediate_4_sub,
       intermediate_4.obj AS intermediate_4_obj
FROM 
(SELECT t.* 
FROM gplus t
JOIN (
    SELECT s.*
    FROM group_sub s
    WHERE s.cnt > 1526) c
    ON t.sub = c.val) AS intermediate_1
 JOIN 
(SELECT t.* 
FROM gplus t
LEFT JOIN sub_obj c ON t.obj = c.val
WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_4
 ON forceorder(intermediate_1.obj = intermediate_4.sub)) AS intermediate_11 JOIN 
(SELECT intermediate_3.sub AS intermediate_3_sub,
       intermediate_3.obj AS intermediate_3_obj,
       intermediate_6.sub AS intermediate_6_sub,
       intermediate_6.obj AS intermediate_6_obj
FROM 
(SELECT t.* 
FROM gplus t
JOIN (
    SELECT s.*
    FROM group_sub s
    WHERE s.cnt > 1526) c
    ON t.sub = c.val) AS intermediate_3
 JOIN 
(SELECT t.* 
FROM gplus t
LEFT JOIN sub_obj c ON t.sub = c.val
WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_6
 ON forceorder(intermediate_3.obj = intermediate_6.obj)) AS intermediate_12 ON forceorder(intermediate_11.intermediate_1_sub = intermediate_12.intermediate_3_sub AND intermediate_11.intermediate_4_obj = intermediate_12.intermediate_6_sub)) AS intermediate_13
;
SELECT COUNT(*)
FROM 
(SELECT intermediate_18.intermediate_15_obj AS intermediate_18_intermediate_15_obj,
       intermediate_18.intermediate_0_sub AS intermediate_18_intermediate_0_sub,
       intermediate_18.intermediate_15_sub AS intermediate_18_intermediate_15_sub,
       intermediate_18.intermediate_0_obj AS intermediate_18_intermediate_0_obj,
       intermediate_19.intermediate_2_obj AS intermediate_19_intermediate_2_obj,
       intermediate_19.intermediate_17_sub AS intermediate_19_intermediate_17_sub,
       intermediate_19.intermediate_17_obj AS intermediate_19_intermediate_17_obj,
       intermediate_19.intermediate_2_sub AS intermediate_19_intermediate_2_sub
FROM 
(SELECT intermediate_0.sub AS intermediate_0_sub,
       intermediate_0.obj AS intermediate_0_obj,
       intermediate_15.sub AS intermediate_15_sub,
       intermediate_15.obj AS intermediate_15_obj
FROM 
(SELECT t.* 
FROM gplus t
JOIN (
    SELECT s.*
    FROM sub_obj s
    WHERE s.cnt > 862) c
    ON t.obj = c.val) AS intermediate_15
 JOIN 
(SELECT t.* 
FROM gplus t
LEFT JOIN group_sub c ON t.sub = c.val
WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 1526) AS intermediate_0
 ON forceorder(intermediate_0.obj = intermediate_15.sub)) AS intermediate_18 JOIN 
(SELECT intermediate_2.sub AS intermediate_2_sub,
       intermediate_2.obj AS intermediate_2_obj,
       intermediate_17.sub AS intermediate_17_sub,
       intermediate_17.obj AS intermediate_17_obj
FROM 
(SELECT t.* 
FROM gplus t
JOIN (
    SELECT s.*
    FROM sub_obj s
    WHERE s.cnt > 862) c
    ON t.sub = c.val) AS intermediate_17
 JOIN 
(SELECT t.* 
FROM gplus t
LEFT JOIN group_sub c ON t.sub = c.val
WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 1526) AS intermediate_2
 ON forceorder(intermediate_2.obj = intermediate_17.obj)) AS intermediate_19 ON forceorder(intermediate_18.intermediate_0_sub = intermediate_19.intermediate_2_sub AND intermediate_18.intermediate_15_obj = intermediate_19.intermediate_17_sub)) AS intermediate_20
;
SELECT COUNT(*)
FROM 
(SELECT intermediate_21.intermediate_2_obj AS intermediate_21_intermediate_2_obj,
       intermediate_21.intermediate_0_sub AS intermediate_21_intermediate_0_sub,
       intermediate_21.intermediate_2_sub AS intermediate_21_intermediate_2_sub,
       intermediate_21.intermediate_0_obj AS intermediate_21_intermediate_0_obj,
       intermediate_22.intermediate_14_obj AS intermediate_22_intermediate_14_obj,
       intermediate_22.intermediate_14_sub AS intermediate_22_intermediate_14_sub,
       intermediate_22.intermediate_16_obj AS intermediate_22_intermediate_16_obj,
       intermediate_22.intermediate_16_sub AS intermediate_22_intermediate_16_sub
FROM 
(SELECT intermediate_14.sub AS intermediate_14_sub,
       intermediate_14.obj AS intermediate_14_obj,
       intermediate_16.sub AS intermediate_16_sub,
       intermediate_16.obj AS intermediate_16_obj
FROM 
(SELECT t.* 
FROM gplus t
LEFT JOIN sub_obj c ON t.sub = c.val
WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_16
 JOIN 
(SELECT t.* 
FROM gplus t
LEFT JOIN sub_obj c ON t.obj = c.val
WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 862) AS intermediate_14
 ON forceorder(intermediate_14.obj = intermediate_16.sub)) AS intermediate_22 JOIN 
(SELECT intermediate_0.sub AS intermediate_0_sub,
       intermediate_0.obj AS intermediate_0_obj,
       intermediate_2.sub AS intermediate_2_sub,
       intermediate_2.obj AS intermediate_2_obj
FROM 
(SELECT t.* 
FROM gplus t
LEFT JOIN group_sub c ON t.sub = c.val
WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 1526) AS intermediate_2
 JOIN 
(SELECT t.* 
FROM gplus t
LEFT JOIN group_sub c ON t.sub = c.val
WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 1526) AS intermediate_0
 ON forceorder(intermediate_0.sub = intermediate_2.sub)) AS intermediate_21 ON forceorder(intermediate_21.intermediate_0_obj = intermediate_22.intermediate_14_sub AND intermediate_21.intermediate_2_obj = intermediate_22.intermediate_16_obj)) AS intermediate_23
;