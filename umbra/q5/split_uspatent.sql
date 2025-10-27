
\o -
\set timeout 5400000
\echo running q5 uspatent

DROP TABLE IF EXISTS group_obj, group_sub, sub_obj;
CREATE TABLE group_obj AS (SELECT obj AS val, COUNT(*) AS cnt FROM uspatent GROUP BY obj ORDER BY cnt DESC LIMIT 100000);
CREATE TABLE group_sub AS (SELECT sub AS val, COUNT(*) AS cnt FROM uspatent GROUP BY sub ORDER BY cnt DESC LIMIT 100000);
CREATE TABLE sub_obj AS (
SELECT group_obj.val AS val, LEAST(group_obj.cnt, group_sub.cnt) AS cnt
FROM group_obj JOIN group_sub
ON group_obj.val = group_sub.val
ORDER BY cnt DESC);
\echo join time:
\set repeat 4
SELECT COUNT(*) FROM uspatent AS t1, uspatent AS t2, uspatent AS t3, uspatent AS t4, uspatent AS t5
WHERE t1.sub = t2.sub AND t1.obj = t5.sub AND t2.obj = t5.obj AND t1.obj = t3.sub AND t3.sub = t5.sub AND t2.obj = t4.obj AND t5.obj = t4.obj AND t3.obj = t4.sub;