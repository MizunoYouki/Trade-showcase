from dataclasses import dataclass

import numpy as np

from trade.model import Exchange, Symbol


@dataclass
class Chunk:
    """
    Executionのチャンク
    """
    exchange: Exchange
    symbol: Symbol
    first_id: int
    first_datetime: np.datetime64
    last_id: int
    last_datetime: np.datetime64
