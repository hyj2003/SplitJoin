
\o -
\set timeout 5400000

\echo running q3 topcats
\set repeat 4
SELECT COUNT(*) FROM topcats AS t1, topcats AS t2, topcats AS t3, topcats AS t4
WHERE t1.sub = t2.sub AND t3.sub = t1.obj AND t3.obj = t4.sub AND t4.obj = t2.obj 
    AND t1.sub % 3 = 1;