SELECT COUNT(*)
FROM {db_name} t1, {db_name} t2, {db_name} t3, {db_name} t4, {db_name} t5
WHERE t1.sub = t2.sub AND t1.obj = t5.sub AND t2.obj = t5.obj AND t1.obj = t3.sub AND t3.sub = t5.sub AND t2.obj = t4.sub AND t5.obj = t4.sub AND t3.obj = t4.obj;