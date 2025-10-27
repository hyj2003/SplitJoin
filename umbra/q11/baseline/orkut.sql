
\o -
\set timeout 5400000
\set repeat 4
\echo running q11 orkut
SELECT COUNT(*) FROM orkut AS t1, orkut AS t2, orkut AS t3, orkut AS t4, orkut AS t5
WHERE t2.sub = t1.obj AND t2.obj = t3.sub AND t3.obj = t4.sub AND t4.obj = t5.sub AND t1.sub = t5.obj;