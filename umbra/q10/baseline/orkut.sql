
\o -
\set timeout 5400000
\set repeat 4
\echo running q10 orkut
SELECT COUNT(*) FROM orkut AS t1, orkut AS t2, orkut AS t3, orkut AS t4, orkut AS t5, orkut AS t6
WHERE t1.obj = t2.sub AND t2.obj = t3.sub AND t3.obj = t4.sub AND t4.obj = t5.sub AND t5.obj = t6.sub AND t6.obj = t1.sub;