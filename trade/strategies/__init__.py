import asyncio
from abc import abstractmethod
from logging import Logger
from typing import List, Union

from trade.broker.declarative.model import NormalizedPositions
from trade.broker.declarative.queue import LifoQueue as LifoQueueClearable
from trade.execution.model import Execution, SwitchedToRealtime
from trade.strategy import BaseStrategy


class Strategies:
    """
    - Premise
      - asset: 1_000
      - strategy-1
        - distribution: 0.7
      - strategy-2
        - distribution: 0.3


    = case: 同じside
      - strategy-1
        - Signal(BUY, 100)
      - strategy-2
        - Signal(BUY, 100)

      - SPS(SP(BUY, 100, size=10=(1_000/100)*0.7 + (1_000/100)*0.3))


    = case: NOTHINGを含むside
      - strategy-1
        - Signal(BUY, 100)
      - strategy-2
        - Signal(NOTHING, 100)

      - SPS(SP(BUY, 100, size=7=(1_000/100)*0.7))


    = case: すべてNOTHING
      - strategy-1
        - Signal(NOTHING, 100)
      - strategy-2
        - Signal(NOTHING, 100)

      - SPS(SP(BUY, 100, size=0)


    = case: 相対するside
      - strategy-1
        - Signal(BUY, 100)
      - strategy-2
        - Signal(SELL, 100)

      - SPS(SP(BUY, 100, size=4=(1_000/100)*0.7 - (1_000/100)*0.3)
    """

    def __init__(self, logger: Logger, strategies: List[BaseStrategy]):
        self._logger = logger
        self._strategies = strategies

    @abstractmethod
    async def positions_distributor(self, execution_queue: 'asyncio.Queue[Union[Execution, SwitchedToRealtime]]',
                                    positions_queue: 'LifoQueueClearable[NormalizedPositions]'):
        """
        Executionを受け取り、各strategyにsignalを求め、その結果を合成してキューへ追加します。

        :param execution_queue:
        :param positions_queue:
        :return:
        """
        raise NotImplementedError
