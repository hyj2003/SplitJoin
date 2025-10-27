
\o -
\set timeout 5400000
\set repeat 4
\echo running q5 gplus
SELECT COUNT(*) FROM gplus AS t1, gplus AS t2, gplus AS t3, gplus AS t4, gplus AS t5
WHERE t1.sub = t2.sub AND t1.obj = t5.sub AND t2.obj = t5.obj AND t1.obj = t3.sub AND t3.sub = t5.sub AND t2.obj = t4.obj AND t5.obj = t4.obj AND t3.obj = t4.sub;