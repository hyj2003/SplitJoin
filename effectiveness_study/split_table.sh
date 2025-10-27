echo "q1
3
t2 t3 obj obj
t3 t1 sub sub
t1 t2 obj sub
2
t1 obj
t2 sub
2
t1 obj
t2 sub
2
t1 obj
t2 sub
2
t1 obj
t2 sub" | python3 -u effectiveness_study/split_table.py

echo "q2
4
t1 t2 obj sub
t2 t3 obj obj
t4 t3 obj sub
t4 t1 sub sub
4
t1 obj
t2 sub
t4 obj
t3 sub
4
t1 obj
t2 sub
t4 obj
t3 sub
4
t1 obj
t2 sub
t4 obj
t3 sub
4
t1 obj
t2 sub
t4 obj
t3 sub" | python3 -u effectiveness_study/split_table.py

echo "q5
8
t1 t2 sub sub
t3 t5 sub sub
t1 t5 obj sub
t2 t4 obj obj
t2 t5 obj obj
t1 t3 obj sub
t3 t4 obj sub
t5 t4 obj obj
4
t1 obj
t5 sub
t3 obj
t4 sub
4
t1 obj
t5 sub
t3 obj
t4 sub
4
t1 obj
t5 sub
t3 obj
t4 sub
4
t1 obj
t5 sub
t3 obj
t4 sub" | python3 -u effectiveness_study/split_table.py