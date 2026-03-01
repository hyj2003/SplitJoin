
\o -
\set timeout 5400000
\echo running q1 gplus
\set repeat 4
SELECT COUNT(*) FROM gplus AS t1, gplus AS t2, gplus AS t3
WHERE t1.obj = t2.sub AND t2.obj = t3.obj AND t3.sub = t1.sub 
    AND ascii(substring(md5(t1.sub), 1, 1)) % 3 = 1;