echo "q1
3
t2 t3 obj obj
t3 t1 sub sub
t1 t2 obj sub" | python3 -u effectiveness_study/split_arbitrarily.py

echo "q2
4
t1 t2 obj sub
t2 t3 obj obj
t4 t3 obj sub
t4 t1 sub sub" | python3 -u effectiveness_study/split_arbitrarily.py

echo "q5
8
t1 t3 obj sub
t1 t5 obj sub
t2 t5 obj obj
t3 t5 sub sub
t2 t4 obj obj
t5 t4 obj obj
t3 t4 obj sub
t1 t2 sub sub" | python3 -u effectiveness_study/split_arbitrarily.py