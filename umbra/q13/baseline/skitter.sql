
\o -
\set timeout 5400000

\echo running q13 skitter
\set repeat 4

SELECT COUNT(*)
FROM skitter t1, skitter t2, skitter t3, skitter t4, skitter t5
WHERE t1.obj = t2.sub AND t2.obj = t3.sub AND t3.obj = t4.obj AND t4.sub = t5.obj AND t5.sub = t1.sub;