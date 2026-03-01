
\o -
\set timeout 5400000

\echo running q2_f topcats
\set repeat 4

SELECT COUNT(*) FROM topcats AS t1, topcats AS t2, topcats AS t3, topcats AS t4
WHERE t2.sub = t1.obj AND t2.obj = t3.obj AND t4.obj = t3.sub AND t4.sub = t1.sub AND t1.sub % 3 = 1;