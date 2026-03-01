
\o -
\set timeout 5400000

\echo running q2_f skitter
\set repeat 4

SELECT COUNT(*) FROM skitter AS t1, skitter AS t2, skitter AS t3, skitter AS t4
WHERE t2.sub = t1.obj AND t2.obj = t3.obj AND t4.obj = t3.sub AND t4.sub = t1.sub AND t1.sub % 3 = 1;