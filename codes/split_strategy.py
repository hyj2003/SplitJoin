import copy
import numpy as np
from typing import List, Tuple, Set
from models import Join
import config
from join_planning import join_phase
from table_split import split_join
from join_labeling import update_HL


def pick_split(joins: List[Join], j_list: List[Tuple[int, str, str]], 
               vis: Set[str], cnt: int):
    """Recursively pick the best split configuration."""
    cand_joins = []
    # First try to find joins in triangles
    for i, j in enumerate(joins):
        if j.table1 in vis or j.table2 in vis:
            continue
        for t in config.triangles:
            if j.join_id in t:
                cand_joins.append((i, j.table1, j.table2))
                break
    # If no triangle joins, consider all joins
    if len(cand_joins) == 0:
        for i, j in enumerate(joins):
            if j.table1 in vis or j.table2 in vis:
                continue
            cand_joins.append((i, j.table1, j.table2))
    
    # Base case: no more candidates
    if len(cand_joins) == 0:
        s = 0
        for j in j_list:
            s = max(s, config.thres_dict[(j[1], j[2])][0])
        if s < config.min_max:
            config.min_j_list = j_list
            config.min_max = s
            config.min_cnt = cnt
        elif s == config.min_max and config.min_cnt > cnt:
            config.min_j_list = j_list
            config.min_max = s
            config.min_cnt = cnt
        return
    
    # Recursive case: try each candidate
    for j in cand_joins:
        pick_split(joins, j_list + [j], vis | {j[1], j[2]}, cnt + 1)


def create_j_list(joins: List[Join]):
    """Create list of joins to split based on heavy hitter analysis."""
    config.min_j_list = []
    config.min_max = 10000000
    config.min_cnt = 10000000
    pick_split(joins, [], set(), 0)
    
    tmp_j_list = []
    for j in config.min_j_list:
        k, d = config.cache_factor[config.thres_dict[(j[1], j[2])][1]]
        if k > 240 or k * 5 < d:
            tmp_j_list.append(j)
        # tmp_j_list.append(j)
    config.min_j_list = tmp_j_list


def split_phase(con, joins: List[Join], light: np.ndarray, 
               j_list: List[Tuple[int, str, str]]):
    """Split phase: recursively split tables based on heavy/light decomposition."""
    if len(j_list) == 0:
        n_light, n_joins = copy.deepcopy(light), copy.deepcopy(joins)
        join_phase(con, n_joins, n_light)
        return
    
    j0 = j_list[0]
    p = j0[0]
    thres, s_tbl = config.thres_dict[(j0[1], j0[2])]
    new_tables = split_join(con, joins[p], s_tbl, thres)
    org_tables = [joins[p].table1, joins[p].table2]
    
    # Process light partition
    n_light, n_joins = copy.deepcopy(light), copy.deepcopy(joins)
    n_light[p] = 1
    for join in n_joins:
        for i, tbl in enumerate(org_tables):
            if join.table1 == tbl:
                join.table1 = new_tables[2 * i]
            if join.table2 == tbl:
                join.table2 = new_tables[2 * i]
    split_phase(con, n_joins, n_light, j_list[1:])
    
    # Process heavy partition
    n_light = update_HL(p, joins, light)
    n_joins = copy.deepcopy(joins)
    n_light[p] = 2
    for join in n_joins:
        for i, tbl in enumerate(org_tables):
            if join.table1 == tbl:
                join.table1 = new_tables[2 * i + 1]
            if join.table2 == tbl:
                join.table2 = new_tables[2 * i + 1]
    split_phase(con, n_joins, n_light, j_list[1:])
