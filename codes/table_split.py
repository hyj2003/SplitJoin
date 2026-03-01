import copy
from typing import List
from models import Join
import config


def split_join(con, join: Join, split_table: str, thres: int) -> List[str]:
    """Split a join into light and heavy parts based on threshold."""
    assert len(join.attr1) == 1 and len(join.attr2) == 1
    print(f"Split table: {join.table1} {join.table2}")
    tables = [join.table1, join.table2]
    attrs = [join.attr1[0], join.attr2[0]]
    new_tables = []
    for i in range(len(attrs)):
        for condition in ["<=", ">"]:
            output_table = f"intermediate_{next(config.intermediate_cnt)}"
            if condition == "<=":
                if not config.enable_filter:
                    config.table_sql[output_table] = f"""
    (SELECT t.* 
    FROM {config.db_name} t
    LEFT JOIN {split_table} c ON t.{attrs[i]} = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END {condition} {thres}) AS {output_table}
    """
                else:
                    config.table_sql[output_table] = f"""
    (SELECT {tables[i]}.* 
    FROM {config.table_sql[tables[i]]}
    LEFT JOIN {split_table} c ON {tables[i]}.{attrs[i]} = c.val
    WHERE CASE WHEN c.cnt IS NULL THEN 0 ELSE c.cnt END {condition} {thres}) AS {output_table}
    """
                config.table_coe[output_table] = 0.99 * config.table_coe[tables[i]]
            else:            
                if not config.enable_filter:    
                    config.table_sql[output_table] = f"""
    (SELECT t.* 
    FROM {config.db_name} t
    JOIN (
        SELECT s.*
        FROM {split_table} s
        WHERE s.cnt {condition} {thres}) c
        ON t.{attrs[i]} = c.val) AS {output_table}
    """
                else:
                    config.table_sql[output_table] = f"""
    (SELECT {tables[i]}.* 
    FROM {config.table_sql[tables[i]]}
    JOIN (
        SELECT s.*
        FROM {split_table} s
        WHERE s.cnt {condition} {thres}) c
        ON {tables[i]}.{attrs[i]} = c.val) AS {output_table}
    """
                config.table_coe[output_table] = 0.01 * config.table_coe[tables[i]]
            config.basetable_dict[output_table] = {tables[i]}
            config.table_size[output_table] = 1
            config.column_dict[output_table] = copy.deepcopy(
                config.column_dict[tables[i]])
            cols = list(config.column_dict[output_table])
            for col in cols:
                config.attr_variable[f"{output_table}.{col}"] = \
                    config.attr_variable[f"{tables[i]}.{col}"]
            config.variable_dict[output_table] = \
                config.variable_dict[f"{tables[i]}"]
            new_tables.append(output_table)
    return new_tables