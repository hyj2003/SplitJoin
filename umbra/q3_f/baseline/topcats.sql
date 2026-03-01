
\o -
\set timeout 5400000

\echo running q3 topcats
\set repeat 4

SELECT COUNT(*)
FROM (topcats AS t3 JOIN topcats AS t1 ON t3.sub = t1.obj)
JOIN (topcats AS t4 JOIN topcats AS t2 ON t4.obj = t2.obj)
ON forceorder(t3.obj = t4.sub AND t1.sub = t2.sub AND t1.sub % 3 = 1);