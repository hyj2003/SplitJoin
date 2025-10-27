SELECT COUNT(*)
FROM {db_name} t1, {db_name} t2, {db_name} t3, {db_name} t4
WHERE t2.sub = t1.obj AND t2.obj = t3.obj AND t4.obj = t3.sub AND t4.sub = t1.sub;