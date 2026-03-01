
\o -
\set timeout 5400000

\echo running q2_f orkut
\set repeat 4

SELECT COUNT(*) FROM orkut AS t1, orkut AS t2, orkut AS t3, orkut AS t4
WHERE t2.sub = t1.obj AND t2.obj = t3.obj AND t4.obj = t3.sub AND t4.sub = t1.sub AND t1.sub % 3 = 1;