
\o -
\set timeout 5400000
\set repeat 4
\echo running q1 topcats
SELECT COUNT(*) FROM topcats AS t1, topcats AS t2, topcats AS t3
WHERE t1.obj = t2.sub AND t2.obj = t3.obj AND t3.sub = t1.sub;