import asyncio
import sys
import time
from argparse import ArgumentParser
from decimal import Decimal
from logging import Logger
from typing import Sequence, AsyncIterator, AsyncIterable

import numpy as np

from trade.log import get_logger
from trade.model import Symbol
from trade.side import Side
from trade.sign import Signal
from trade.strategy import BaseStrategy
from trade.broker.declarative.model import Position


class BaseDatastore:
    v: int

    def __init__(self, logger: Logger):
        self.logger = logger
        self.v = 0


class RealtimeDatastore(AsyncIterable[int]):

    def __init__(self, logger):
        self._base = BaseDatastore(logger)

    async def __aiter__(self) -> AsyncIterator[int]:
        while True:
            await asyncio.sleep(1)

            self._base.logger.info(f'born: {self._base.v}')

            yield self._base.v

            self._base.v += 1


class PlaybackDatastore(AsyncIterable[int]):

    def __init__(self, logger):
        self._base = BaseDatastore(logger)

    async def __aiter__(self) -> AsyncIterator[int]:
        while True:
            self._base.logger.info(f'born: {self._base.v}')

            yield self._base.v

            self._base.v += 1


class ZipStream(AsyncIterable[int]):

    def __init__(self, logger: Logger, upstream: AsyncIterable[int]):
        self._logger = logger
        self._upstream = upstream

        self._count = 0

    async def __aiter__(self) -> AsyncIterator[int]:
        neighbor = None

        async for v in self._upstream:
            self._logger.info(f'PROCESS zip    : {v}')

            self._count += 1

            if neighbor is not None:
                yield v

                if self._count % 3 == 0:
                    self._logger.info('heavy to zip, blocking sleep 3 seconds')
                    time.sleep(3)
            else:
                self._logger.info('PROCESS zip    : insufficient to zip, does not sending')

            if v % 2 == 1:
                neighbor = v


class PrintStream(AsyncIterable[int]):

    def __init__(self, logger: Logger, upstream: AsyncIterable[int]):
        self._logger = logger
        self._upstream = upstream

    async def __aiter__(self) -> AsyncIterator[int]:
        async for v in self._upstream:
            self._logger.info(f'PROCESS printer: {v}')

            yield v


class MockStrategy(BaseStrategy):

    def make_decision(self, v: int) -> Signal:
        return Signal(
            side=Side.BUY, price=Decimal('100'),
            decision_at=np.datetime64('now', 'ns', utc=True),
            origin_at=np.datetime64('now', 'ns', utc=True), reason='TODO', v=v,
        )


class SignalSynthesizer:

    def __init__(self, logger: Logger, strategies: Sequence[MockStrategy]):
        self._logger = logger
        self._strategies = strategies

    def _synthesize(self, v: int) -> Position:
        signals = [strategy.make_decision(v) for strategy in self._strategies]
        signal = signals[-1]
        return Position(
            symbol=Symbol.FXBTCJPY, side=signal.side, price=signal.price, size=Decimal('0.1'),
        )

    async def synthesize_infinitely(self,
                                    v_queue: 'asyncio.Queue[int]',
                                    positions_queue: 'asyncio.Queue[Position]'):
        while True:
            v = await v_queue.get()
            self._logger.info(f'synthesizer    : {v}')

            position = self._synthesize(v)
            positions_queue.put_nowait(position)

    def synthesize(self, v) -> Position:
        return self._synthesize(v)


class Broker:

    def __init__(self, logger: Logger, candidate_queue: 'asyncio.Queue[Position]'):
        self._logger = logger
        self._candidate_queue = candidate_queue
        self._newest_queue: 'asyncio.LifoQueue[Position]' = asyncio.LifoQueue()

    async def observe_infinitely(self):
        while True:
            requirement: Position = await self._candidate_queue.get()

            self._newest_queue.put_nowait(requirement)

    async def trade_infinitely(self, positions_queue: 'asyncio.Queue[Position]'):
        while True:
            position = await positions_queue.get()
            self._logger.info(f'trader got position: {position}')

            self._logger.info(f'trader facing heavy network I/O, async sleep 10 seconds')
            await asyncio.sleep(10)
            self._logger.info(f'trader done, position: {position}')

    async def concurrent_runner(self):
        pass
        # await asyncio.gather(
        #     self.
        # )


async def scenario(logger: Logger, playback: bool = False):
    """
    WebSocketクライアントが受け取った約定、またはSQLiteデータベースからプレイバックする約定を、
    パイプラインで連結して処理する方法のシナリオ

    想定している処理は、並行実行が不可能だ（前段が終わらなければ、次段を開始できない）。
    したがって、パイプラインは最初から最後まで同期とした。
    必要な時に非同期にすればよい。
    """

    v_queue: asyncio.Queue[int] = asyncio.Queue()
    positions_queue: asyncio.Queue[Position] = asyncio.Queue()

    processor = PrintStream(
        logger=logger,
        upstream=ZipStream(
            logger=logger,
            upstream=playback and PlaybackDatastore(logger=logger) or RealtimeDatastore(logger=logger)
        )
    )

    async def subscribe(q: 'asyncio.Queue[int]'):
        async for v in processor:
            logger.info(f'PROCESS queue  : {v}')

            q.put_nowait(v)

    await asyncio.gather(
        asyncio.create_task(subscribe(q=v_queue)),
        asyncio.create_task(
            SignalSynthesizer(logger=logger, strategies=[MockStrategy(), MockStrategy()]).synthesize_infinitely(
                v_queue=v_queue, positions_queue=positions_queue
            )
        ),
        asyncio.create_task(Broker(logger=logger).trade_infinitely(positions_queue=positions_queue)),
    )


if __name__ == '__main__':
    p = ArgumentParser()
    p.add_argument('--realtime', action='store_true')
    p.add_argument('--playback', action='store_true')
    args = p.parse_args()

    _logger = get_logger(__name__, stream=sys.stdout,
                         _format='%(asctime)s:%(levelname)s: %(message)s')

    if args.realtime:
        asyncio.run(scenario(_logger, playback=False))

    elif args.playback:
        asyncio.run(scenario(_logger, playback=True))

    else:
        raise Exception('Argument realtime or playback required.')
