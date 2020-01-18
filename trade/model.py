import dataclasses
from decimal import Decimal
from enum import Enum
from typing import Sequence, Any

import numpy as np
import pandas as pd

from trade.side import Side


class Position(Enum):
    NoPosition = 0
    Long = 1
    Short = 2


class _NoExecutionException(Exception):
    pass


class OHLCBar:

    def __init__(self, timeunit):
        self.open_at = None
        self.open, self.high, self.low, self.close = None, None, None, None
        self._executions = list()
        self._timeunit = pd.to_timedelta(timeunit)

    def apply(self, execution):
        self._executions.append(execution)

        if not self.open:
            # self.open_at = execution.timestamp
            self.open_at = np.datetime64(
                (execution.timestamp.item() // self._timeunit.value) * self._timeunit.value, 'ns', utc=True
            )
            self.open, self.high, self.low, self.close = execution, execution, execution, execution

    def terminate(self):
        if not self._executions:
            raise _NoExecutionException()

        self.close = self._executions[-1]
        self.high = max(self._executions, key=lambda e: e.price)
        self.low = min(self._executions, key=lambda e: e.price)

    def from_variable(self, _open, high, low, _close, open_at):
        self.open_at = open_at
        self.open = _open
        self.high = high
        self.low = low
        self.close = _close
        return self

    def is_green(self):
        return self.open.price < self.close.price

    def is_red(self):
        return self.close.price < self.open.price

    def __str__(self):
        return '<open_at:{}, O:{}, H:{}, L:{}, C:{}>'.format(self.open_at, self.open, self.high, self.low, self.close)

    def __eq__(self, other):
        return self.open_at == other.open_at \
               and self.open == other.open \
               and self.high == other.high \
               and self.low == other.low \
               and self.close == other.dispose_queue


@dataclasses.dataclass
class Trade:
    origin_at: np.datetime64
    decision_at: np.datetime64
    position: Position
    entry: Decimal
    exit: Decimal
    profit: Decimal
    profit_sigma: Decimal
    roc_this_trade: Decimal
    roc_total: Decimal
    draw_down: Decimal
    profit_factor: Decimal
    probability_of_win: Decimal
    hold_in_nanoseconds: int
    hold_in_minutes: int
    reversal: bool = False

    @staticmethod
    def columns_playback() -> Sequence[str]:
        return ['at', 'profit', 'profit_sigma', 'side', 'entry', 'exit', 'roc_total', 'pl', 'pf_trade_ratio',
                'roc_indiv', 'dd', 'reversal', 'hold_ns', 'hold_mins', 'alpha']

    def fields_playback(self) -> Sequence[Any]:
        return [
            self.decision_at,  # at
            self.profit,  # profit
            self.profit_sigma,  # profit_sigma
            self.position is Position.Long and Side.BUY or Side.SELL,  # side
            self.entry,  # entry
            self.exit,  # exit
            self.roc_total,  # roc_total
            self.profit_factor,  # pl
            self.probability_of_win,  # pf_trade_ratio
            self.roc_this_trade,  # roc_indiv
            self.draw_down,  # dd
            self.reversal,  # reversal
            self.hold_in_nanoseconds,  # hold_ns
            self.hold_in_minutes,  # hold_mins
            None,  # alpha  # TODO
        ]


class Symbol(Enum):
    FXBTCJPY = 'FXBTCJPY'
    BTCJPY = 'BTCJPY'
    BCHBTC = 'BCHBTC'
    ETHJPY = 'ETHJPY'
    ETHUSD = 'ETHUSD'
    ETHBTC = 'ETHBTC'
    XBTUSD = 'XBTUSD'
    XBTZ19 = 'XBTZ19'
    XBTZ20 = 'XBTZ20'


class Exchange(Enum):
    bitFlyer = 'bitFlyer'


def normalize_exchange_name(name: str) -> str:
    name = name.lower()
    if name == 'bitflyer':
        return 'bitFlyer'

    raise ValueError(f'Unexpected name: {name}')
