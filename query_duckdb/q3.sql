SELECT COUNT(*)
FROM {db_name} t1, {db_name} t2, {db_name} t3, {db_name} t4
WHERE t1.sub = t2.sub AND t3.sub = t1.obj AND t3.obj = t4.sub AND t4.obj = t2.obj;