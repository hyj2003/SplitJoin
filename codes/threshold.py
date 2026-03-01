from typing import Tuple


def get_threshold(con, s_tbl: str) -> int:
    """Compute threshold."""
    # df = con.execute(f"SELECT * FROM {s_tbl}").fetchdf()
    # cnts = df['cnt'].values
    # l, r, threshold = 0, len(cnts) - 1, 0
    # while l <= r:
    #     mid = (l + r) // 2
    #     if mid >= cnts[mid]:
    #         threshold, r = cnts[mid], mid - 1
    #     else:
    #         l = mid + 1
    # return threshold
    return con.execute(f"""
        SELECT COALESCE(MAX(cnt), 0)
        FROM (
            SELECT
                cnt,
                row_number() OVER (ORDER BY cnt DESC) - 1 AS idx
            FROM {s_tbl}
        )
        WHERE cnt <= idx
    """).fetchone()[0]