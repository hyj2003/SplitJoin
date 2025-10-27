
\o -
\set timeout 5400000
\echo running q1 wgpb

DROP TABLE IF EXISTS group_obj, group_sub, sub_obj;
CREATE TABLE group_obj AS (SELECT obj AS val, COUNT(*) AS cnt FROM wgpb GROUP BY obj ORDER BY cnt DESC LIMIT 100000);
CREATE TABLE group_sub AS (SELECT sub AS val, COUNT(*) AS cnt FROM wgpb GROUP BY sub ORDER BY cnt DESC LIMIT 100000);
CREATE TABLE sub_obj AS (
SELECT group_obj.val AS val, LEAST(group_obj.cnt, group_sub.cnt) AS cnt
FROM group_obj JOIN group_sub
ON group_obj.val = group_sub.val
ORDER BY cnt DESC);
\echo join time:
\set repeat 4

SELECT COUNT(*)
FROM 
(SELECT intermediate_3.sub AS intermediate_3_sub,
       intermediate_3.obj AS intermediate_3_obj,
       intermediate_4.intermediate_1_sub AS intermediate_4_intermediate_1_sub,
       intermediate_4.intermediate_1_obj AS intermediate_4_intermediate_1_obj,
       intermediate_4.t3_sub AS intermediate_4_t3_sub,
       intermediate_4.t3_obj AS intermediate_4_t3_obj
FROM 
(SELECT t.* 
FROM wgpb t
JOIN (
    SELECT s.*
    FROM sub_obj s
    WHERE s.cnt > 180) c
    ON t.sub = c.val) AS intermediate_3
 JOIN 
(SELECT intermediate_1.sub AS intermediate_1_sub,
       intermediate_1.obj AS intermediate_1_obj,
       t3.sub AS t3_sub,
       t3.obj AS t3_obj
FROM 
(SELECT t.* 
FROM wgpb t
JOIN (
    SELECT s.*
    FROM sub_obj s
    WHERE s.cnt > 180) c
    ON t.obj = c.val) AS intermediate_1
 JOIN wgpb AS t3 ON forceorder(intermediate_1.sub = t3.sub)) AS intermediate_4 ON forceorder(intermediate_3.sub = intermediate_4.intermediate_1_obj AND intermediate_3.obj = intermediate_4.t3_obj)) AS intermediate_5
;
SELECT COUNT(*)
FROM 
(SELECT intermediate_6.intermediate_0_obj AS intermediate_6_intermediate_0_obj,
       intermediate_6.intermediate_0_sub AS intermediate_6_intermediate_0_sub,
       intermediate_6.intermediate_2_obj AS intermediate_6_intermediate_2_obj,
       intermediate_6.intermediate_2_sub AS intermediate_6_intermediate_2_sub,
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
WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 180) AS intermediate_2
 JOIN 
(SELECT t.* 
FROM wgpb t
LEFT JOIN sub_obj c ON t.obj = c.val
WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END <= 180) AS intermediate_0
 ON forceorder(intermediate_0.obj = intermediate_2.sub)) AS intermediate_6 ON forceorder(intermediate_6.intermediate_2_obj = t3.obj AND intermediate_6.intermediate_0_sub = t3.sub)) AS intermediate_7
;