
\o -
\set timeout 5400000
\set repeat 4
\echo running q8 uspatent
SELECT COUNT(*) FROM uspatent AS t1, uspatent AS t2, uspatent AS t3, uspatent AS t4, uspatent AS t5, uspatent AS t6, uspatent AS t7, uspatent AS t8
WHERE t1.obj = t2.sub AND t1.sub = t3.obj AND t2.obj = t3.sub AND t5.sub = t4.obj AND t4.sub = t6.obj AND t6.sub = t5.obj AND t2.sub = t8.obj AND t5.sub = t7.obj AND t8.sub = t7.sub AND t2.obj = t5.obj;