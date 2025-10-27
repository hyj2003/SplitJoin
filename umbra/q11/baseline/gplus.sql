
\o -
\set timeout 5400000
\set repeat 4
\echo running q11 gplus
SELECT COUNT(*) FROM gplus AS t1, gplus AS t2, gplus AS t3, gplus AS t4, gplus AS t5
WHERE t2.sub = t1.obj AND t2.obj = t3.sub AND t3.obj = t4.sub AND t4.obj = t5.sub AND t1.sub = t5.obj;