
\o -
\set timeout 5400000

\echo running q3 skitter
\set repeat 4

SELECT COUNT(*) FROM skitter AS t1, skitter AS t2, skitter AS t3, skitter AS t4
WHERE t1.sub = t2.sub AND t3.sub = t1.obj AND t3.obj = t4.sub AND t4.obj = t2.obj 
    AND t1.sub % 3 = 1;