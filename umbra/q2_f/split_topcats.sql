
\o -
\set timeout 5400000
\echo running q2_f topcats

\set repeat 4
\echo warmup begin:
SELECT COUNT(DISTINCT obj) as unique_objs
FROM topcats;
SELECT COUNT(DISTINCT sub) as unique_subs
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

CREATE TEMP TABLE group_obj_f AS (
    SELECT obj AS val, COUNT(*) AS cnt 
    FROM (SELECT sub, obj FROM topcats WHERE sub % 3 = 1) AS t1 
    GROUP BY obj 
    ORDER BY cnt DESC 
    LIMIT 5000
);

CREATE TEMP TABLE group_sub_f AS (
    SELECT sub AS val, COUNT(*) AS cnt 
    FROM (SELECT sub, obj FROM topcats WHERE sub % 3 = 1) AS t1 
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
    FROM group_obj_f AS t1 JOIN group_sub AS t2
    ON t1.val = t2.val
    ORDER BY cnt DESC
);
CREATE TEMP TABLE sub_obj_3 AS (
    SELECT t1.val AS val, LEAST(t1.cnt, t2.cnt) AS cnt
    FROM group_sub_f AS t1 JOIN group_sub AS t2
    ON t1.val = t2.val
    ORDER BY cnt DESC
);
\echo join time:

SELECT COUNT(*)
FROM 
(SELECT intermediate_8.intermediate_2_obj AS intermediate_8_intermediate_2_obj,
       intermediate_8.intermediate_0_obj AS intermediate_8_intermediate_0_obj,
       intermediate_8.intermediate_0_sub AS intermediate_8_intermediate_0_sub,
       intermediate_8.intermediate_2_sub AS intermediate_8_intermediate_2_sub,
       intermediate_9.intermediate_6_sub AS intermediate_9_intermediate_6_sub,
       intermediate_9.intermediate_4_obj AS intermediate_9_intermediate_4_obj,
       intermediate_9.intermediate_6_obj AS intermediate_9_intermediate_6_obj,
       intermediate_9.intermediate_4_sub AS intermediate_9_intermediate_4_sub
FROM 
(SELECT intermediate_0.obj AS intermediate_0_obj,
       intermediate_0.sub AS intermediate_0_sub,
       intermediate_2.obj AS intermediate_2_obj,
       intermediate_2.sub AS intermediate_2_sub
FROM 
    (SELECT t1.* 
    FROM (SELECT sub, obj FROM topcats WHERE sub % 3 = 1) AS t1
    LEFT JOIN sub_obj_0 c ON t1.obj = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 146) AS intermediate_0
     JOIN 
    (SELECT t2.* 
    FROM topcats AS t2
    LEFT JOIN sub_obj_0 c ON t2.sub = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 146) AS intermediate_2
     ON forceorder(intermediate_0.obj = intermediate_2.sub)) AS intermediate_8 JOIN 
(SELECT intermediate_4.obj AS intermediate_4_obj,
       intermediate_4.sub AS intermediate_4_sub,
       intermediate_6.obj AS intermediate_6_obj,
       intermediate_6.sub AS intermediate_6_sub
FROM 
    (SELECT t4.* 
    FROM topcats AS t4
    LEFT JOIN sub_obj_2 c ON t4.obj = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 146) AS intermediate_6
     JOIN 
    (SELECT t3.* 
    FROM topcats AS t3
    LEFT JOIN sub_obj_2 c ON t3.sub = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 146) AS intermediate_4
     ON forceorder(intermediate_4.sub = intermediate_6.obj)) AS intermediate_9 ON forceorder(intermediate_8.intermediate_2_obj = intermediate_9.intermediate_4_obj AND intermediate_8.intermediate_0_sub = intermediate_9.intermediate_6_sub)) AS intermediate_10
        ;
SELECT COUNT(*)
FROM 
(SELECT intermediate_11.intermediate_2_obj AS intermediate_11_intermediate_2_obj,
       intermediate_11.intermediate_5_sub AS intermediate_11_intermediate_5_sub,
       intermediate_11.intermediate_5_obj AS intermediate_11_intermediate_5_obj,
       intermediate_11.intermediate_2_sub AS intermediate_11_intermediate_2_sub,
       intermediate_12.intermediate_0_obj AS intermediate_12_intermediate_0_obj,
       intermediate_12.intermediate_0_sub AS intermediate_12_intermediate_0_sub,
       intermediate_12.intermediate_7_sub AS intermediate_12_intermediate_7_sub,
       intermediate_12.intermediate_7_obj AS intermediate_12_intermediate_7_obj
FROM 
(SELECT intermediate_0.obj AS intermediate_0_obj,
       intermediate_0.sub AS intermediate_0_sub,
       intermediate_7.obj AS intermediate_7_obj,
       intermediate_7.sub AS intermediate_7_sub
FROM 
    (SELECT t4.* 
    FROM topcats AS t4
    JOIN (
        SELECT s.*
        FROM sub_obj_2 s
        WHERE s.cnt > 146) c
        ON t4.obj = c.val) AS intermediate_7
     JOIN 
    (SELECT t1.* 
    FROM (SELECT sub, obj FROM topcats WHERE sub % 3 = 1) AS t1
    LEFT JOIN sub_obj_0 c ON t1.obj = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 146) AS intermediate_0
     ON forceorder(intermediate_0.sub = intermediate_7.sub)) AS intermediate_12 JOIN 
(SELECT intermediate_2.obj AS intermediate_2_obj,
       intermediate_2.sub AS intermediate_2_sub,
       intermediate_5.obj AS intermediate_5_obj,
       intermediate_5.sub AS intermediate_5_sub
FROM 
    (SELECT t3.* 
    FROM topcats AS t3
    JOIN (
        SELECT s.*
        FROM sub_obj_2 s
        WHERE s.cnt > 146) c
        ON t3.sub = c.val) AS intermediate_5
     JOIN 
    (SELECT t2.* 
    FROM topcats AS t2
    LEFT JOIN sub_obj_0 c ON t2.sub = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 146) AS intermediate_2
     ON forceorder(intermediate_2.obj = intermediate_5.obj)) AS intermediate_11 ON forceorder(intermediate_11.intermediate_2_sub = intermediate_12.intermediate_0_obj AND intermediate_11.intermediate_5_sub = intermediate_12.intermediate_7_obj)) AS intermediate_13
        ;
SELECT COUNT(*)
FROM 
(SELECT intermediate_18.intermediate_14_obj AS intermediate_18_intermediate_14_obj,
       intermediate_18.intermediate_3_obj AS intermediate_18_intermediate_3_obj,
       intermediate_18.intermediate_3_sub AS intermediate_18_intermediate_3_sub,
       intermediate_18.intermediate_14_sub AS intermediate_18_intermediate_14_sub,
       intermediate_19.intermediate_16_sub AS intermediate_19_intermediate_16_sub,
       intermediate_19.intermediate_1_sub AS intermediate_19_intermediate_1_sub,
       intermediate_19.intermediate_1_obj AS intermediate_19_intermediate_1_obj,
       intermediate_19.intermediate_16_obj AS intermediate_19_intermediate_16_obj
FROM 
(SELECT intermediate_1.obj AS intermediate_1_obj,
       intermediate_1.sub AS intermediate_1_sub,
       intermediate_16.obj AS intermediate_16_obj,
       intermediate_16.sub AS intermediate_16_sub
FROM 
    (SELECT t1.* 
    FROM (SELECT sub, obj FROM topcats WHERE sub % 3 = 1) AS t1
    JOIN (
        SELECT s.*
        FROM sub_obj_0 s
        WHERE s.cnt > 146) c
        ON t1.obj = c.val) AS intermediate_1
     JOIN 
    (SELECT t4.* 
    FROM topcats AS t4
    LEFT JOIN sub_obj_2 c ON t4.obj = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 146) AS intermediate_16
     ON forceorder(intermediate_1.sub = intermediate_16.sub)) AS intermediate_19 JOIN 
(SELECT intermediate_3.obj AS intermediate_3_obj,
       intermediate_3.sub AS intermediate_3_sub,
       intermediate_14.obj AS intermediate_14_obj,
       intermediate_14.sub AS intermediate_14_sub
FROM 
    (SELECT t2.* 
    FROM topcats AS t2
    JOIN (
        SELECT s.*
        FROM sub_obj_0 s
        WHERE s.cnt > 146) c
        ON t2.sub = c.val) AS intermediate_3
     JOIN 
    (SELECT t3.* 
    FROM topcats AS t3
    LEFT JOIN sub_obj_2 c ON t3.sub = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 146) AS intermediate_14
     ON forceorder(intermediate_3.obj = intermediate_14.obj)) AS intermediate_18 ON forceorder(intermediate_18.intermediate_3_sub = intermediate_19.intermediate_1_obj AND intermediate_18.intermediate_14_sub = intermediate_19.intermediate_16_obj)) AS intermediate_20
        ;
SELECT COUNT(*)
FROM 
(SELECT intermediate_21.intermediate_15_sub AS intermediate_21_intermediate_15_sub,
       intermediate_21.intermediate_3_obj AS intermediate_21_intermediate_3_obj,
       intermediate_21.intermediate_3_sub AS intermediate_21_intermediate_3_sub,
       intermediate_21.intermediate_15_obj AS intermediate_21_intermediate_15_obj,
       intermediate_22.intermediate_17_obj AS intermediate_22_intermediate_17_obj,
       intermediate_22.intermediate_1_sub AS intermediate_22_intermediate_1_sub,
       intermediate_22.intermediate_1_obj AS intermediate_22_intermediate_1_obj,
       intermediate_22.intermediate_17_sub AS intermediate_22_intermediate_17_sub
FROM 
(SELECT intermediate_1.obj AS intermediate_1_obj,
       intermediate_1.sub AS intermediate_1_sub,
       intermediate_17.obj AS intermediate_17_obj,
       intermediate_17.sub AS intermediate_17_sub
FROM 
    (SELECT t1.* 
    FROM (SELECT sub, obj FROM topcats WHERE sub % 3 = 1) AS t1
    JOIN (
        SELECT s.*
        FROM sub_obj_0 s
        WHERE s.cnt > 146) c
        ON t1.obj = c.val) AS intermediate_1
     JOIN 
    (SELECT t4.* 
    FROM topcats AS t4
    JOIN (
        SELECT s.*
        FROM sub_obj_2 s
        WHERE s.cnt > 146) c
        ON t4.obj = c.val) AS intermediate_17
     ON forceorder(intermediate_1.sub = intermediate_17.sub)) AS intermediate_22 JOIN 
(SELECT intermediate_3.obj AS intermediate_3_obj,
       intermediate_3.sub AS intermediate_3_sub,
       intermediate_15.obj AS intermediate_15_obj,
       intermediate_15.sub AS intermediate_15_sub
FROM 
    (SELECT t3.* 
    FROM topcats AS t3
    JOIN (
        SELECT s.*
        FROM sub_obj_2 s
        WHERE s.cnt > 146) c
        ON t3.sub = c.val) AS intermediate_15
     JOIN 
    (SELECT t2.* 
    FROM topcats AS t2
    JOIN (
        SELECT s.*
        FROM sub_obj_0 s
        WHERE s.cnt > 146) c
        ON t2.sub = c.val) AS intermediate_3
     ON forceorder(intermediate_3.obj = intermediate_15.obj)) AS intermediate_21 ON forceorder(intermediate_21.intermediate_3_sub = intermediate_22.intermediate_1_obj AND intermediate_21.intermediate_15_sub = intermediate_22.intermediate_17_obj)) AS intermediate_23
        ;