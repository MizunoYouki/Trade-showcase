from decimal import Decimal

import numpy as np

from trade.side import Side


class Signal:

    def __init__(self,
                 side: Side,
                 price: Decimal,
                 decision_at: np.datetime64,
                 origin_at: np.datetime64,
                 reason: str,
                 **extras):
        self.side = side
        self.price = price
        self.decision_at = decision_at
        self.origin_at = origin_at
        self.reason = reason
        self.extras = extras

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return f'{self.__class__.__name__}' \
               f"({self.side}, price={str(self.price)}, decision_at={str(self.decision_at)}" \
               f", origin_at={str(self.origin_at)}, reason={str(self.reason)}, extras=**{self.extras})"
