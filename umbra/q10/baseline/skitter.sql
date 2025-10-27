
\o -
\set timeout 5400000
\set repeat 4
\echo running q10 skitter
SELECT COUNT(*) FROM skitter AS t1, skitter AS t2, skitter AS t3, skitter AS t4, skitter AS t5, skitter AS t6
WHERE t1.obj = t2.sub AND t2.obj = t3.sub AND t3.obj = t4.sub AND t4.obj = t5.sub AND t5.obj = t6.sub AND t6.obj = t1.sub;