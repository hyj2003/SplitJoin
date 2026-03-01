import numpy as np
from typing import List, Set
from models import Join
import config


def update_HL(pos: int, joins: List[Join], light: np.ndarray) -> np.ndarray:
    assert len(joins[pos].attr1) == 1 and len(joins[pos].attr2) == 1
    t1, t2 = joins[pos].table1, joins[pos].table2
    a1, a2 = joins[pos].attr1[0], joins[pos].attr2[0]
    res = np.array(light)
    for i in range(len(joins)):
        if i == pos:
            continue
        if (len(config.basetable_dict[joins[i].table1]) > 1 or 
            len(config.basetable_dict[joins[i].table2]) > 1):
            continue
        for a in joins[i].attr1:
            if ((a != a1 and joins[i].table1 == t1) or 
                (a != a2 and joins[i].table1 == t2)):
                res[i] = 1
                break
        for a in joins[i].attr2:
            if ((a != a1 and joins[i].table2 == t1) or 
                (a != a2 and joins[i].table2 == t2)):
                res[i] = 1
                break
    return res


def label_joins(joins: List[Join]) -> None:
    cnt = 0
    fa = np.zeros(len(joins) + 1, dtype=np.int32)
    for i in range(len(joins) + 1):
        fa[i] = i
    
    def getf(x: int) -> np.int32:
        while x != fa[x]:
            fa[x] = fa[fa[x]]
            x = fa[x]
        return x
    
    for i in range(0, len(joins)):
        for j in range(0, i): 
            if ({f"{joins[i].table1}.{joins[i].attr1[0]}", 
                 f"{joins[i].table2}.{joins[i].attr2[0]}"} & 
                {f"{joins[j].table1}.{joins[j].attr1[0]}", 
                 f"{joins[j].table2}.{joins[j].attr2[0]}"}):
                fa[getf(i + 1)] = getf(j + 1)

    for i in range(0, len(joins)):
        if joins[getf(i + 1) - 1].label == 0:
            cnt += 1
            joins[getf(i + 1) - 1].label = cnt
        joins[i].label = joins[getf(i + 1) - 1].label


def find_triangles(joins: List[Join]) -> None:
    for i in range(len(joins)):
        for j in range(i + 1, len(joins)):
            if (not ({joins[i].table1, joins[i].table2} &
                    {joins[j].table1, joins[j].table2}) or 
                joins[i].label == joins[j].label):
                continue
            for k in range(j + 1, len(joins)):
                if ((not ({joins[i].table1, joins[i].table2} &
                         {joins[k].table1, joins[k].table2})) or 
                    (not ({joins[j].table1, joins[j].table2} &
                         {joins[k].table1, joins[k].table2}))):
                    continue
                s = frozenset([joins[i].table1, joins[i].table2, 
                              joins[j].table1, joins[j].table2, 
                              joins[k].table1, joins[k].table2])
                if (len({joins[i].label, joins[j].label, joins[k].label}) == 3 
                    and len(s) == 3):
                    config.triangles.add(frozenset([joins[i].join_id, 
                                                   joins[j].join_id, 
                                                   joins[k].join_id]))
