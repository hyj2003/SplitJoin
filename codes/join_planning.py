import copy
import numpy as np
from typing import List
from models import Join
import config
from join_execution import execute_join


def join_phase(con, joins: List[Join], light: np.ndarray):
    """Compute optimal join order using dynamic programming."""
    table_list = []
    for j in joins:
        if j.table1 not in table_list:
            table_list.append(j.table1)
        if j.table2 not in table_list:
            table_list.append(j.table2)
    n = len(table_list)
    
    # Dynamic programming arrays
    # f[s]: min cost, g[s]: current size, h[s]: previous state
    f = np.full(1 << n, -1, dtype=float)
    g = np.full(1 << n, -1, dtype=float)
    h = np.zeros(1 << n, dtype=int)
    
    # Initialize single tables
    for i in range(n):
        f[1 << i] = 0
        g[1 << i] = config.table_coe[table_list[i]]

    joins_copy = copy.deepcopy(joins)
    for i, j in enumerate(joins):
        if light[i] == 2:
            j.factor = 1

    def compute_size(s: int, t: int):
        """Compute cost and size for joining two subsets."""
        var_s, var_t = set(), set()
        for i in range(n):
            if s >> i & 1 != 0:
                var_s = var_s | config.variable_dict[table_list[i]]
            if t >> i & 1 != 0:
                var_t = var_t | config.variable_dict[table_list[i]]
        
        res_f = min(g[s], g[t]) * 1.0 + max(g[s], g[t]) * 0.1 + f[s] + f[t]
        res_g = 0
        
        if var_s == var_t:
            res_g = min(g[s], g[t])
        elif var_s <= var_t:
            res_g = g[t]
        elif var_s >= var_t:
            res_g = g[s]
        else:
            res_g = 1
            for i, j in enumerate(joins):
                idx1 = table_list.index(j.table1)
                idx2 = table_list.index(j.table2)
                in_s1, in_s2 = (s >> idx1) & 1, (s >> idx2) & 1
                in_t1, in_t2 = (t >> idx1) & 1, (t >> idx2) & 1
                if (in_s1 and in_t2) or (in_s2 and in_t1):
                    if light[i] == 1:
                        res_g = min(res_g, j.factor_L)
                    else:
                        res_g = min(res_g, j.factor)
            res_g = res_g * g[s] * g[t]
        return res_f, res_g
    
    # Dynamic programming loop
    for s in range(1, 1 << n):
        t = s
        while t:
            t = (t - 1) & s
            if t == 0:
                break
            cur_f, cur_g = compute_size(s ^ t, t)
            g[s] = cur_g if g[s] < 0 else min(g[s], cur_g)
            if f[s] < 0:
                f[s], h[s] = cur_f, t
            else:
                if cur_f < f[s]:
                    f[s], h[s] = cur_f, t

    def gen_plan(s: int, indent: int = 0):
        """Generate execution plan from DP solution."""
        prefix = "  " * indent
        if h[s] == 0:
            idx = int(np.log2(s))
            return (table_list[idx], 
                   f"{prefix}Table: {config.basetable_dict[table_list[idx]]}")
        
        t = h[s]
        l_table, l_plan = gen_plan(s ^ t, indent + 2)
        r_table, r_plan = gen_plan(t, indent + 2)
        
        pos = 0
        nonlocal joins
        for i, j in enumerate(joins):
            if j.table1 == l_table and j.table2 == r_table:
                pos = i
                break
            if j.table2 == l_table and j.table1 == r_table:
                pos = i
                j.table1, j.table2 = j.table2, j.table1
                j.attr1, j.attr2 = j.attr2, j.attr1
                break
        
        vl, vr = g[s ^ t], g[t]
        sl, sr = bin(s ^ t).count('1'), bin(t).count('1')
        if sl != sr:
            if sl == 3 and len(config.variable_dict[l_table]) == 3:
                sl = 1.5
            if sr == 3 and len(config.variable_dict[r_table]) == 3:
                sr = 1.5
            vl, vr = sl, sr
        
        output_table, joins = execute_join(pos, joins, vl, vr)
        plan = f"{prefix}JOIN\n"
        if vl < vr:
            plan += f"{'  ' * (indent + 1)}Left:\n{r_plan}\n"
            plan += f"{'  ' * (indent + 1)}Right:\n{l_plan}"
        else:
            plan += f"{'  ' * (indent + 1)}Left:\n{l_plan}\n"
            plan += f"{'  ' * (indent + 1)}Right:\n{r_plan}"
        return output_table, plan
    
    final_table, plan = gen_plan((1 << n) - 1)

    config.final_joins.append(f"""
SELECT COUNT(*)
FROM {config.table_sql[final_table]}
        """)

    joins = joins_copy
    return
