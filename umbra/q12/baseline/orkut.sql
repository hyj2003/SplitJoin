
\o -
\set timeout 5400000

\echo running q12 orkut
\set repeat 4
SELECT COUNT(*)
FROM orkut t1, orkut t2, orkut t3, orkut t4, orkut t5, orkut t6
WHERE t2.sub = t1.obj AND t2.obj = t3.sub AND t3.obj = t4.sub AND t4.obj = t5.sub AND t1.sub = t5.obj AND t2.sub = t6.obj AND t1.obj = t6.obj AND t5.sub = t6.sub AND t4.obj = t6.sub;