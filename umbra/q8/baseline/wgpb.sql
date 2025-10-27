
\o -
\set timeout 5400000
\set repeat 4
\echo running q8 wgpb
SELECT COUNT(*) FROM wgpb AS t1, wgpb AS t2, wgpb AS t3, wgpb AS t4, wgpb AS t5, wgpb AS t6, wgpb AS t7, wgpb AS t8
WHERE t1.obj = t2.sub AND t1.sub = t3.obj AND t2.obj = t3.sub AND t5.sub = t4.obj AND t4.sub = t6.obj AND t6.sub = t5.obj AND t2.sub = t8.obj AND t5.sub = t7.obj AND t8.sub = t7.sub AND t2.obj = t5.obj;