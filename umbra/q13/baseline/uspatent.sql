
\o -
\set timeout 5400000

\echo running q13 uspatent
\set repeat 4

SELECT COUNT(*)
FROM uspatent t1, uspatent t2, uspatent t3, uspatent t4, uspatent t5
WHERE t1.obj = t2.sub AND t2.obj = t3.sub AND t3.obj = t4.obj AND t4.sub = t5.obj AND t5.sub = t1.sub;