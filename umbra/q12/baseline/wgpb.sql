
\o -
\set timeout 5400000

\echo running q12 wgpb
\set repeat 4

SELECT COUNT(*)
FROM wgpb t1, wgpb t2, wgpb t3, wgpb t4, wgpb t5, wgpb t6
WHERE t2.sub = t1.obj AND t2.obj = t3.sub AND t3.obj = t4.sub AND t4.obj = t5.sub AND t1.sub = t5.obj AND t2.sub = t6.obj AND t1.obj = t6.obj AND t5.sub = t6.sub AND t4.obj = t6.sub;