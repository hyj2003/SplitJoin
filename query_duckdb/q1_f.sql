-- Filters are added in the run time.
SELECT COUNT(*)
FROM {db_name} t1, {db_name} t2, {db_name} t3
WHERE t1.obj = t2.sub AND t2.obj = t3.obj AND t3.sub = t1.sub