echo "q1_f
3
t1 t2 obj sub
t2 t3 obj obj
t3 t1 sub sub" | python3 -u codes/main.py --run-umbra --enable-filters

echo "q2_f
4
t1 t2 obj sub
t2 t3 obj obj
t3 t4 sub obj
t4 t1 sub sub" | python3 -u codes/main.py --run-umbra --enable-filters

echo "q3_f
4
t1 t2 sub sub
t3 t1 sub obj
t3 t4 obj sub
t4 t2 obj obj" | python3 -u codes/main.py --run-umbra --enable-filters

echo "q1
3
t1 t2 obj sub
t2 t3 obj obj
t3 t1 sub sub" | python3 -u codes/main.py --run-umbra

echo "q2
4
t1 t2 obj sub
t2 t3 obj obj
t4 t3 obj sub
t4 t1 sub sub" | python3 -u codes/main.py --run-umbra

echo "q3
4
t1 t2 sub sub
t3 t1 sub obj
t3 t4 obj sub
t4 t2 obj obj" | python3 -u codes/main.py --run-umbra

echo "q4
8
t1 t2 sub sub
t1 t5 obj sub
t2 t5 obj obj
t1 t3 obj sub
t3 t5 sub sub
t2 t4 obj sub
t5 t4 obj sub
t3 t4 obj obj" | python3 -u codes/main.py --run-umbra

echo "q5
8
t1 t2 sub sub
t1 t5 obj sub
t2 t5 obj obj
t1 t3 obj sub
t3 t5 sub sub
t2 t4 obj obj
t5 t4 obj obj
t3 t4 obj sub" | python3 -u codes/main.py --run-umbra

echo "q6
10
t1 t2 sub sub
t1 t6 sub sub
t2 t5 obj obj
t2 t6 sub sub
t3 t5 sub sub
t3 t6 obj obj
t3 t4 obj obj
t6 t4 obj obj
t1 t3 obj sub
t2 t4 obj sub" | python3 -u codes/main.py --run-umbra

echo "q7
8
t3 t5 obj sub
t4 t6 obj sub
t5 t6 obj obj
t4 t5 sub sub
t1 t3 obj sub
t1 t2 sub sub
t2 t3 obj obj
t2 t4 obj sub" | python3 -u codes/main.py --run-umbra

echo "q8
10
t1 t2 obj sub
t1 t3 sub obj
t2 t3 obj sub
t5 t4 sub obj
t4 t6 sub obj
t6 t5 sub obj
t2 t8 sub obj
t5 t7 sub obj
t8 t7 sub sub
t2 t5 obj obj" | python3 -u codes/main.py --run-umbra

echo "q9
9
t1 t2 sub sub
t1 t3 obj sub
t2 t4 obj sub
t3 t4 obj obj
t3 t5 obj sub
t4 t6 obj sub
t5 t6 sub sub
t5 t7 obj sub
t6 t7 obj obj" | python3 -u codes/main.py --run-umbra

echo "q10
6
t1 t2 obj sub
t2 t3 obj sub
t3 t4 obj sub
t4 t5 obj sub
t5 t6 obj sub
t6 t1 obj sub" | python3 -u codes/main.py --run-umbra

echo "q11
5
t2 t1 sub obj
t2 t3 obj sub
t3 t4 obj sub
t4 t5 obj sub
t1 t5 sub obj" | python3 -u codes/main.py --run-umbra

echo "q12
9
t2 t1 sub obj
t2 t3 obj sub
t3 t4 obj sub
t4 t5 obj sub
t1 t5 sub obj
t2 t6 sub obj 
t1 t6 obj obj
t5 t6 sub sub
t4 t6 obj sub" | python3 -u codes/main.py --run-umbra

echo "q13
5
t1 t2 obj sub
t2 t3 obj sub
t3 t4 obj obj
t4 t5 sub obj
t5 t1 sub sub" | python3 -u codes/main.py --run-umbra