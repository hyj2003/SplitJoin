
\o -
\set timeout 5400000
\set repeat 4
\echo running q4 wgpb
SELECT COUNT(*) FROM wgpb AS t1, wgpb AS t2, wgpb AS t3, wgpb AS t4, wgpb AS t5
WHERE t1.sub = t2.sub AND t1.obj = t5.sub AND t2.obj = t5.obj AND t1.obj = t3.sub AND t3.sub = t5.sub AND t2.obj = t4.sub AND t5.obj = t4.sub AND t3.obj = t4.obj;