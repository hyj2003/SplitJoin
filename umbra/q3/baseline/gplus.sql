
\o -
\set timeout 5400000
\set repeat 4
\echo running q3 gplus
SELECT COUNT(*) FROM gplus AS t1, gplus AS t2, gplus AS t3, gplus AS t4
WHERE t1.sub = t2.sub AND t3.sub = t1.obj AND t3.obj = t4.sub AND t4.obj = t2.obj;