SELECT COUNT(*)
FROM {db_name} t1, {db_name} t2, {db_name} t3, {db_name} t4, {db_name} t5, {db_name} t6
WHERE t1.sub = t2.sub AND t1.obj = t5.sub AND t1.sub = t6.sub AND t2.obj = t5.obj AND t2.sub = t6.sub AND t3.sub = t5.sub AND t3.obj = t6.obj AND t3.obj = t4.obj AND t6.obj = t4.obj AND t4.sub = t5.obj;