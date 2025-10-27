SELECT COUNT(*)
FROM {db_name} t1, {db_name} t2, {db_name} t3, {db_name} t4, 
     {db_name} t5, {db_name} t6, {db_name} t7, {db_name} t8
WHERE t1.obj = t2.sub AND t1.sub = t3.obj AND t2.obj = t3.sub AND t5.sub = t4.obj AND t4.sub = t6.obj AND t6.sub = t5.obj AND t2.sub = t8.obj AND t5.sub = t7.obj AND t8.sub = t7.sub AND t2.obj = t5.obj;