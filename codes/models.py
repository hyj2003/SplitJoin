from dataclasses import dataclass
from typing import List


@dataclass
class Join:
    table1: str
    table2: str
    attr1: List[str]
    attr2: List[str]
    label: int
    join_id: int
    factor: float
    factor_L: float
