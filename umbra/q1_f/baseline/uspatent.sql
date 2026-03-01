
\o -
\set timeout 5400000
\echo running q1 uspatent
\set repeat 4

SELECT COUNT(*) FROM uspatent AS t1, uspatent AS t2, uspatent AS t3
WHERE t1.obj = t2.sub AND t2.obj = t3.obj AND t3.sub = t1.sub AND t1.sub % 3 = 1;