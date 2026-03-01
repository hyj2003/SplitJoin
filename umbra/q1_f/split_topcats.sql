
\o -
\set timeout 5400000
\echo running q1_f topcats

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
    FROM group_sub_f AS t1 JOIN group_sub AS t2
    ON t1.val = t2.val
    ORDER BY cnt DESC
);
\echo join time:

SELECT COUNT(*)
FROM 
(SELECT intermediate_4.intermediate_0_obj AS intermediate_4_intermediate_0_obj,
       intermediate_4.intermediate_0_sub AS intermediate_4_intermediate_0_sub,
       intermediate_4.intermediate_2_obj AS intermediate_4_intermediate_2_obj,
       intermediate_4.intermediate_2_sub AS intermediate_4_intermediate_2_sub,
       t3.obj AS t3_obj,
       t3.sub AS t3_sub
FROM topcats AS t3 JOIN 
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
     ON forceorder(intermediate_0.obj = intermediate_2.sub)) AS intermediate_4 ON forceorder(intermediate_4.intermediate_2_obj = t3.obj AND intermediate_4.intermediate_0_sub = t3.sub)) AS intermediate_5
        ;
SELECT COUNT(*)
FROM 
(SELECT intermediate_3.obj AS intermediate_3_obj,
       intermediate_3.sub AS intermediate_3_sub,
       intermediate_6.t3_obj AS intermediate_6_t3_obj,
       intermediate_6.t3_sub AS intermediate_6_t3_sub,
       intermediate_6.intermediate_1_sub AS intermediate_6_intermediate_1_sub,
       intermediate_6.intermediate_1_obj AS intermediate_6_intermediate_1_obj
FROM 
    (SELECT t2.* 
    FROM topcats AS t2
    JOIN (
        SELECT s.*
        FROM sub_obj_0 s
        WHERE s.cnt > 146) c
        ON t2.sub = c.val) AS intermediate_3
     JOIN 
(SELECT intermediate_1.obj AS intermediate_1_obj,
       intermediate_1.sub AS intermediate_1_sub,
       t3.obj AS t3_obj,
       t3.sub AS t3_sub
FROM 
    (SELECT t1.* 
    FROM (SELECT sub, obj FROM topcats WHERE sub % 3 = 1) AS t1
    JOIN (
        SELECT s.*
        FROM sub_obj_0 s
        WHERE s.cnt > 146) c
        ON t1.obj = c.val) AS intermediate_1
     JOIN topcats AS t3 ON forceorder(intermediate_1.sub = t3.sub)) AS intermediate_6 ON forceorder(intermediate_3.sub = intermediate_6.intermediate_1_obj AND intermediate_3.obj = intermediate_6.t3_obj)) AS intermediate_7
        ;