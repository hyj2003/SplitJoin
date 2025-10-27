
\o -
\set timeout 5400000
\set repeat 4
\echo running q2 orkut
SELECT COUNT(*) FROM orkut AS t1, orkut AS t2, orkut AS t3, orkut AS t4
WHERE t2.sub = t1.obj AND t2.obj = t3.obj AND t4.obj = t3.sub AND t4.sub = t1.sub;