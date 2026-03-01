
\o -
\set timeout 5400000

\echo running q2_f uspatent
\set repeat 4

SELECT COUNT(*) FROM uspatent AS t1, uspatent AS t2, uspatent AS t3, uspatent AS t4
WHERE t2.sub = t1.obj AND t2.obj = t3.obj AND t4.obj = t3.sub AND t4.sub = t1.sub AND t1.sub % 3 = 1;