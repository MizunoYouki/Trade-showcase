import asyncio
from collections import deque
from decimal import Decimal
from logging import Logger
from typing import Union, List, Deque

import numpy as np

from trade.broker.declarative.model import NormalizedPositions, Position
from trade.broker.declarative.queue import LifoQueue as LifoQueueClearable
from trade.execution.model import SwitchedToRealtime, Execution
from trade.model import Symbol
from trade.side import Side
from trade.sign import Signal
from trade.strategies import Strategies
from trade.strategy import BaseStrategy


class StrategiesStub(Strategies):
    """
    ストラテジー数が1の、Strategies
    """

    def __init__(self, logger: Logger, strategies: List[BaseStrategy], size: Decimal):
        if len(strategies) != 1:
            raise ValueError(f'{self.__class__.__name__} requires an only one strategy')

        super().__init__(logger, strategies)

        self._size = size
        self._got_sw: bool = False
        self._prevs: Deque[Union[Execution, SwitchedToRealtime]] = deque(maxlen=2)
        self._prev_signal: Signal = Signal(side=Side.NOTHING, price=Decimal('NaN'), decision_at=np.datetime64('NaT'),
                                           origin_at=np.datetime64('NaT'), reason='initialized signal')

    async def positions_distributor(self, execution_queue: 'asyncio.Queue[Union[Execution, SwitchedToRealtime]]',
                                    positions_queue: 'LifoQueueClearable[NormalizedPositions]'):
        while True:
            execution: Union[Execution, SwitchedToRealtime] = await execution_queue.get()
            self._prevs.append(execution)

            if isinstance(execution, SwitchedToRealtime):
                self._got_sw = True
                continue

            signal: Signal = self._strategies[0].make_decision(execution)
            if signal.side in (Side.NOTHING, Side.CONTINUE):
                self._prev_signal = signal
                continue

            if self._got_sw:
                self._logger.info(f'signal: {signal}, execution: {execution}, prevs: {self._prevs}')

                if self._prev_signal.side == signal.side:
                    self._logger.info('imitating same side')
                    continue

                positions_queue.put_nowait(NormalizedPositions({
                    Symbol.FXBTCJPY: Position(
                        symbol=execution.symbol, side=signal.side, price=signal.price, size=self._size
                    )
                }))
                self._prev_signal = signal

