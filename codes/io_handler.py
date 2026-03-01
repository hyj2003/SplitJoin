from typing import List
from models import Join
import config


def read_joins() -> List[Join]:
    n = int(input())
    relations = []
    for i in range(n):
        line = input().strip()
        parts = line.split()
        if len(parts) != 4:
            print("Error: Each line must have 4 parts")
            continue
        table1, table2, attr1, attr2 = parts
        config.join_id_cnt += 1
        relations.append(Join(table1, table2, [attr1], [attr2], 0, 
                            config.join_id_cnt, 0, 0))
    return relations
