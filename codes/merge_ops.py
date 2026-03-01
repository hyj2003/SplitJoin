from typing import Tuple, Dict, List
from models import Join
import config
from utils import get_column_names


def merge(join: Join, size1: float, size2: float) -> Tuple[str, Dict[str, str]]:
    """Merge two tables based on join conditions."""
    output_table = f"intermediate_{next(config.intermediate_cnt)}"
    cols1 = get_column_names(join.table1)
    cols2 = get_column_names(join.table2)
    rename_map = {}

    select_list = []
    
    config.column_dict[output_table] = set()
    for col in cols1:
        new_name = f"{join.table1}_{col}"
        select_list.append(f"{join.table1}.{col} AS {new_name}")
        rename_map[f"{join.table1}.{col}"] = new_name
        config.attr_variable[f"{output_table}.{new_name}"] = \
            config.attr_variable[f"{join.table1}.{col}"]
        config.column_dict[output_table].add(new_name)
    
    for col in cols2:
        new_name = f"{join.table2}_{col}"
        select_list.append(f"{join.table2}.{col} AS {new_name}")
        rename_map[f"{join.table2}.{col}"] = new_name
        config.attr_variable[f"{output_table}.{new_name}"] = \
            config.attr_variable[f"{join.table2}.{col}"]
        config.column_dict[output_table].add(new_name)
    
    select_clause = ",\n       ".join(select_list) 
    join_conditions = [
        f"{join.table1}.{a1} = {join.table2}.{a2}"
        for a1, a2 in zip(join.attr1, join.attr2)
    ]
    join_clause = " AND ".join(join_conditions)
    config.variable_dict[output_table] = \
        config.variable_dict[join.table1] | config.variable_dict[join.table2]
    
    if size1 < size2:
        join.table1, join.table2 = join.table2, join.table1
        join.attr1, join.attr2 = join.attr2, join.attr1
    if not config.run_umbra_flag:
        config.table_sql[output_table] = f"""
(SELECT {select_clause}
FROM {config.table_sql[join.table1]}
JOIN {config.table_sql[join.table2]} ON {join_clause}) AS {output_table}
"""
    else:
        config.table_sql[output_table] = f"""
(SELECT {select_clause}
FROM {config.table_sql[join.table2]} JOIN {config.table_sql[join.table1]} ON forceorder({join_clause})) AS {output_table}"""
    # print(config.table_sql[output_table])
    config.basetable_dict[output_table] = \
        config.basetable_dict[join.table1] | config.basetable_dict[join.table2]
    config.table_size[output_table] = \
        config.table_size[join.table1] + config.table_size[join.table2]
    config.table_coe[output_table] = \
        config.table_coe[join.table1] * config.table_coe[join.table2] * join.factor
    
    return output_table, rename_map


def update_join_with_merge(joins: List[Join], pos: int, 
                           output_table: str, rename_map: Dict[str, str]) -> None:
    """Update join specifications after a merge operation."""
    merged_join = joins[pos]
    for join in joins:
        for i in range(len(join.attr1)):
            old_ref = f"{join.table1}.{join.attr1[i]}"
            if old_ref in rename_map:
                join.table1 = output_table
                join.attr1[i] = rename_map[old_ref]
        for i in range(len(join.attr2)):
            old_ref = f"{join.table2}.{join.attr2[i]}"
            if old_ref in rename_map:
                join.table2 = output_table
                join.attr2[i] = rename_map[old_ref]
