
\o -
\set timeout 5400000

\echo running q12 gplus
\set repeat 4
SELECT COUNT(*)
FROM gplus t1, gplus t2, gplus t3, gplus t4, gplus t5, gplus t6
WHERE t2.sub = t1.obj AND t2.obj = t3.sub AND t3.obj = t4.sub AND t4.obj = t5.sub AND t1.sub = t5.obj AND t2.sub = t6.obj AND t1.obj = t6.obj AND t5.sub = t6.sub AND t4.obj = t6.sub;