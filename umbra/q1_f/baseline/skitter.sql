
\o -
\set timeout 5400000
\echo running q1 skitter
\set repeat 4

SELECT COUNT(*) FROM skitter AS t1, skitter AS t2, skitter AS t3
WHERE t1.obj = t2.sub AND t2.obj = t3.obj AND t3.sub = t1.sub AND t1.sub % 3 = 1;