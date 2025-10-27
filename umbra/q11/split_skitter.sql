
\o -
\set timeout 5400000
\echo running q11 skitter

DROP TABLE IF EXISTS group_obj, group_sub, sub_obj;
CREATE TABLE group_obj AS (SELECT obj AS val, COUNT(*) AS cnt FROM skitter GROUP BY obj ORDER BY cnt DESC LIMIT 100000);
CREATE TABLE group_sub AS (SELECT sub AS val, COUNT(*) AS cnt FROM skitter GROUP BY sub ORDER BY cnt DESC LIMIT 100000);
CREATE TABLE sub_obj AS (
SELECT group_obj.val AS val, LEAST(group_obj.cnt, group_sub.cnt) AS cnt
FROM group_obj JOIN group_sub
ON group_obj.val = group_sub.val
ORDER BY cnt DESC);
\echo join time:
\set repeat 4
SELECT COUNT(*) FROM skitter AS t1, skitter AS t2, skitter AS t3, skitter AS t4, skitter AS t5
WHERE t2.sub = t1.obj AND t2.obj = t3.sub AND t3.obj = t4.sub AND t4.obj = t5.sub AND t1.sub = t5.obj;