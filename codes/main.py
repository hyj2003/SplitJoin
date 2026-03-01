import os
import copy
from io_handler import read_joins
from join_labeling import label_joins
from database_ops import run_query
import config
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--enable-filters",
        action="store_true",
        help="enable filters"
    )
    parser.add_argument(
        "--run-umbra",
        action="store_true",
        help="run umbra queries"
    )
    args = parser.parse_args()
    if args.enable_filters:
        print("filters enabled")
        config.enable_filter = True
    else:
        print("filters disabled")

    if args.run_umbra:
        print("For umbra")
        config.run_umbra_flag = True
        config.ratio = 100
    else:
        config.ratio = 25
        print("For duckdb")

    config.query_name = input()
    joins = read_joins()
    label_joins(joins)
    tmp_joins = copy.deepcopy(joins)

    # Database names to process
    db_names = ['wgpb', 'orkut', 'gplus', 'uspatent', 'skitter', 'topcats']
    # Set CPU affinity
    os.sched_setaffinity(0, set(range(32, 64)))
    
    for db_name in db_names:
        print(f"running {config.query_name} {db_name}")
        print("len(joins)", len(joins))
        joins = copy.deepcopy(tmp_joins)
        config.umbra_sql_file_path = f"./umbra/{config.query_name}/split_{db_name}.sql"
        config.db_name = db_name
        run_query(joins, db_name)


if __name__ == "__main__":
    main()