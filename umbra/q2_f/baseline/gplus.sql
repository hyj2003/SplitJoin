
\o -
\set timeout 5400000

\echo running q2_f gplus
\set repeat 4
SELECT COUNT(*) FROM gplus AS t1, gplus AS t2, gplus AS t3, gplus AS t4
WHERE t2.sub = t1.obj AND t2.obj = t3.obj AND t4.obj = t3.sub AND t4.sub = t1.sub 
    AND ascii(substring(md5(t1.sub), 1, 1)) % 3 = 1;