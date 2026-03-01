SELECT COUNT(*)
FROM {db_name} t1, {db_name} t2, {db_name} t3, {db_name} t4, {db_name} t5
WHERE t1.obj = t2.sub AND t2.obj = t3.sub AND t3.obj = t4.obj AND t4.sub = t5.obj AND t5.sub = t1.sub;