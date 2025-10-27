
\o -
\set timeout 5400000
\set repeat 4
\echo running q10 topcats
SELECT COUNT(*) FROM topcats AS t1, topcats AS t2, topcats AS t3, topcats AS t4, topcats AS t5, topcats AS t6
WHERE t1.obj = t2.sub AND t2.obj = t3.sub AND t3.obj = t4.sub AND t4.obj = t5.sub AND t5.obj = t6.sub AND t6.obj = t1.sub;