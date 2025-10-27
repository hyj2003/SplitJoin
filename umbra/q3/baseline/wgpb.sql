
\o -
\set timeout 5400000
\set repeat 4
\echo running q3 wgpb
SELECT COUNT(*) FROM wgpb AS t1, wgpb AS t2, wgpb AS t3, wgpb AS t4
WHERE t1.sub = t2.sub AND t3.sub = t1.obj AND t3.obj = t4.sub AND t4.obj = t2.obj;