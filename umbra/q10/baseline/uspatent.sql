
\o -
\set timeout 5400000
\set repeat 4
\echo running q10 uspatent
SELECT COUNT(*) FROM uspatent AS t1, uspatent AS t2, uspatent AS t3, uspatent AS t4, uspatent AS t5, uspatent AS t6
WHERE t1.obj = t2.sub AND t2.obj = t3.sub AND t3.obj = t4.sub AND t4.obj = t5.sub AND t5.obj = t6.sub AND t6.obj = t1.sub;