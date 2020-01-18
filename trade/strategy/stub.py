import random
from logging import Logger
from typing import Optional

import pandas as pd

from trade.execution.model import Execution
from trade.model import Symbol
from trade.side import Side
from trade.sign import Signal
from trade.strategy import BaseStrategy


class RandomDotenStrategy(BaseStrategy):

    def __init__(self, logger: Logger, time_window: str):
        self._logger = logger
        self._prev: Optional[Execution] = None
        self._prev_2ago: Optional[Execution] = None
        self._time_window: pd.Timedelta = pd.to_timedelta(time_window)
        self._timeunits: int = 0

    def make_decision(self, execution: Execution) -> Signal:
        if execution.symbol is not Symbol.FXBTCJPY:
            return Signal(side=Side.NOTHING, price=execution.price, decision_at=execution.timestamp,
                          origin_at=execution.timestamp, reason='Ignoring, not a primary symbol')

        self._prev_2ago, self._prev = self._prev, execution

        if not self._prev_2ago:
            return Signal(side=Side.NOTHING, price=execution.price, decision_at=execution.timestamp,
                          origin_at=execution.timestamp, reason='Insufficient: first execution')

        if not self._prev:
            return Signal(side=Side.NOTHING, price=execution.price, decision_at=execution.timestamp,
                          origin_at=execution.timestamp, reason='Insufficient: second execution')

        timeunits: int = execution.timestamp.item() // self._time_window.value
        if self._timeunits and self._timeunits == timeunits:
            self._timeunits = timeunits
            return Signal(side=Side.CONTINUE, price=execution.price, decision_at=execution.timestamp,
                          origin_at=self._prev.timestamp, reason='same time unit')

        side = random.choice([Side.BUY, Side.SELL])
        signal = Signal(side=side, price=execution.price, decision_at=execution.timestamp,
                        origin_at=self._prev.timestamp, reason='chosen randomly')
        self._timeunits = timeunits
        return signal
