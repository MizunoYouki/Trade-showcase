import asyncio
import logging
import random
import string
import sys
from argparse import ArgumentParser
from asyncio import Task, Queue
from decimal import Decimal
from enum import Enum, auto
from logging import Logger
from typing import List, Type
from urllib.error import HTTPError

import numpy as np
from tenacity import retry, before_sleep_log, retry_if_exception_type

from trade.execution.model import Execution
from trade.log import get_logger
from trade.model import Symbol
from trade.side import Side
from trade.sign import Signal
from trade.strategy import BaseStrategy
from trade.broker.declarative.model import Position, Positions
from trade.broker.declarative.queue import LifoQueue as LifoQueueClearable
from trade.test_helper import make_execution


class LoggingAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return f'[{self.extra["name"]}] {msg}', kwargs


class CountdownPositions(Positions):
    def __init__(self, positions: List[Position], pre_retries: int = 0):
        super().__init__(positions)

        self.remains = pre_retries

    def decrement(self):
        self.remains -= 1


def inspect_tasks(logger):
    tasks = asyncio.all_tasks()
    for t in tasks:
        logger.debug(f'task: {t}')


class State(Enum):
    Idle = auto()
    Provisioning = auto()


class Broker:

    def __init__(self, logger: Logger):
        self._logger = logger
        self._client: Client = Client(self._logger)
        self._state: State = State.Idle
        self.trader_tasks_started_later: List[Task] = list()
        self._semaphore = asyncio.Semaphore()

    async def _init(self, synthesized_queue: 'LifoQueueClearable[Positions]'):
        self._candidate_queue = synthesized_queue
        self._newest_queue: 'LifoQueueClearable[NormalizedPositions]' = LifoQueueClearable(self._logger)
        self._trader_stopped = asyncio.Event()

    @staticmethod
    async def create_broker(logger: Logger,
                            synthesized_queue: 'LifoQueueClearable[Positions]'):
        broker = Broker(logger)
        await broker._init(synthesized_queue=synthesized_queue)
        return broker

    async def start_new_trader(self, trader_task: Task):
        logger = LoggingAdapter(self._logger, {'name': self.start_new_trader.__name__})

        while True:
            logger.info(f'started to detect the trader should be cancelled: {trader_task}')

            await self._trader_stopped.wait()
            logger.info(f'detect that the trader should be cancelled: {trader_task}')

            trader_task.cancel()
            logger.info(f'finished to cancel the trader: {trader_task}')

            new_trader_task: Task = asyncio.create_task(self.trader())
            self.trader_tasks_started_later.append(new_trader_task)
            logger.info(f'finished to start the new trader: {new_trader_task}')

            trader_task = new_trader_task

            self._trader_stopped.clear()

    async def observer(self):
        logger = LoggingAdapter(self._logger, {'name': self.observer.__name__})

        while True:
            logger.info('waiting requirement')

            requirement: CountdownPositions = await self._candidate_queue.get()
            if self._state is State.Idle:
                logger.info(f'got: {requirement}')

                self._state_to(State.Provisioning)
            else:
                logger.info(f'OOD, got: {requirement}')

                inspect_tasks(logger)
                self._trader_stopped.set()

            self._newest_queue.put_nowait(requirement)

    async def trader(self):
        logger = LoggingAdapter(self._logger, {'name': self.trader.__name__})
        _id: str = ''

        while True:
            logger.info('waiting requirement')

            requirement: CountdownPositions = await self._newest_queue.get()
            _id = self._generate_id()
            logger.info(f'@{_id} got: {requirement}')

            self._newest_queue.clear()

            async with self._semaphore:
                code, response = await self._phase_1(_id, requirement)
            logger.info(f'@{_id} code: {code}, response: {response}')

            self._state_to(State.Idle)

    async def _phase_1(self, _id: str, requirement: CountdownPositions):
        return await self._client.send_request(_id, requirement)

    def _generate_id(self):
        return ''.join(random.choice(string.hexdigits) for _ in range(8))

    def _state_to(self, state: State):
        self._state = state
        self._logger.info(f'state: {self._state}')


class Client:
    __logger: Logger

    def __init__(self, logger: Logger):
        self._logger = logger

    async def send_request(self, _id: str, requirement: CountdownPositions):
        logger = LoggingAdapter(self._logger, {'name': self.send_request.__name__})

        @retry(before_sleep=before_sleep_log(self._logger, logging.WARNING), retry=retry_if_exception_type(HTTPError))
        async def opener() -> [int, dict]:
            logger.info(f'@{_id} started to send POST request: {requirement}')

            if 0 < requirement.remains:
                await asyncio.sleep(0.1)
                requirement.decrement()
                raise HTTPError('https://example.org/', 503, 'This is mock service unavailable', hdrs=None, fp=None)

            await asyncio.sleep(0.9)
            requirement.decrement()
            logger.info(f'@{_id} successfully finished to request')

            return 200, {'about': 'This is response', 'requirement': requirement}

        try:
            code, response = await opener()
            return code, response
        except asyncio.CancelledError:
            logger.info(f'@{_id} caught CE, aborted')
            raise


class Strategies:

    def __init__(self, logger: Logger,
                 strategies: List['MockBaseStrategy'],
                 execution_queue: 'Queue[Execution]',  # Not in use in design.
                 synthesized_queue: 'LifoQueueClearable[CountdownPositions]'):
        self._logger = logger
        self._strategies = strategies
        self._execution_queue = execution_queue
        self._synthesized_queue = synthesized_queue
        self._synthesized_positions: List[CountdownPositions] = list()

    async def position_synthesizer(self):
        strategy = self._strategies[0]
        execution_dummy: Execution = make_execution(symbol=Symbol.FXBTCJPY, _id=1)

        while True:
            if not strategy.signals_mock:
                return

            signal = strategy.make_decision(execution_dummy)
            self._synthesized_queue.put_nowait(CountdownPositions(
                positions=[Position(
                    symbol=Symbol.FXBTCJPY, side=signal.side, price=signal.price, size=Decimal(str(signal.price)),
                )],
                pre_retries='pre_retries' in signal.extras and signal.extras['pre_retries'] or 0)
            )
            if 'await_after' in signal.extras:
                await asyncio.sleep(signal.extras['await_after'])


class MockBaseStrategy(BaseStrategy):

    def __init__(self, logger: Logger, signals_mock: List[Signal]):
        self._logger = logger
        self.signals_mock = signals_mock
        self.timeout = 0

    def make_decision(self, execution: Execution) -> Signal:
        return self.signals_mock.pop(0)


class StrategyCommon(MockBaseStrategy):

    def __init__(self, logger: Logger):
        super().__init__(logger, signals_mock=[
            Signal(side=Side.BUY, price=Decimal('1'), decision_at=np.datetime64('now', 'ns'),
                   origin_at=np.datetime64('now', 'ns'), reason='mock')
        ])
        self.timeout = (0.9 + 0.1)


class StrategyRetry(MockBaseStrategy):

    def __init__(self, logger: Logger):
        super().__init__(logger, signals_mock=[
            Signal(side=Side.BUY, price=Decimal('1'), decision_at=np.datetime64('now', 'ns'),
                   origin_at=np.datetime64('now', 'ns'), reason='mock', pre_retries=3)
        ])
        self.timeout = 0.1 * 3 + (0.9 + 0.1)


class StrategyOOD(MockBaseStrategy):

    def __init__(self, logger: Logger):
        super().__init__(logger, signals_mock=[
            Signal(side=Side.BUY, price=Decimal('1'), decision_at=np.datetime64('now', 'ns'),
                   origin_at=np.datetime64('now', 'ns'), reason='mock', await_after=0.1),
            Signal(side=Side.BUY, price=Decimal('2'), decision_at=np.datetime64('now', 'ns'),
                   origin_at=np.datetime64('now', 'ns'), reason='mock', await_after=0.1),
            Signal(side=Side.BUY, price=Decimal('3'), decision_at=np.datetime64('now', 'ns'),
                   origin_at=np.datetime64('now', 'ns'), reason='mock'),
        ])
        self.timeout = 0.1 + 0.1 + (0.9 + 0.1)


def assert_general(trader_tasks_started_later: List[Task],
                   trader_task: Task,
                   position_synthesizer_task: Task,
                   start_new_trader_task: Task,
                   observer_task: Task):
    assert 0 == len(trader_tasks_started_later)
    assert not trader_task.cancelled()
    assert position_synthesizer_task.done()
    assert not start_new_trader_task.done()
    assert not observer_task.done()


def assert_ood(trader_tasks_started_later: List[Task],
               trader_task: Task,
               position_synthesizer_task: Task,
               start_new_trader_task: Task,
               observer_task: Task):
    assert 2 == len(trader_tasks_started_later)
    assert trader_task.cancelled()
    assert position_synthesizer_task.done()
    assert not start_new_trader_task.done()
    assert not observer_task.done()


async def main(logger: Logger,
               strategy_classes: List[Type['MockBaseStrategy']],
               assert_fn):
    execution_queue: asyncio.Queue[Execution] = asyncio.Queue()
    synthesized_queue: LifoQueueClearable[CountdownPositions] = LifoQueueClearable(logger)
    _strategies = [klass(logger) for klass in strategy_classes]
    strategies = Strategies(
        logger,
        strategies=_strategies,
        execution_queue=execution_queue,
        synthesized_queue=synthesized_queue
    )
    broker = await Broker.create_broker(logger, synthesized_queue)

    trader_task: Task = asyncio.create_task(broker.trader())
    position_synthesizer_task: Task = asyncio.create_task(strategies.position_synthesizer())
    start_new_trader_task: Task = asyncio.create_task(broker.start_new_trader(trader_task))
    observer_task: Task = asyncio.create_task(broker.observer())
    done, pending = await asyncio.wait(
        [
            position_synthesizer_task,
            start_new_trader_task,
            observer_task,
            trader_task,
        ],
        return_when=asyncio.ALL_COMPLETED,
        timeout=sum(strategy.timeout for strategy in _strategies)
    )

    inspect_tasks(logger)

    logger = LoggingAdapter(logger, {'name': 'inspection'})
    for trader_task_born_later in broker.trader_tasks_started_later:
        # TODO: write test (An only one task is pending, others are `done`.)
        logger.info(f'trader task born later: {trader_task_born_later}')
    for task_done in done:
        logger.info(f'done: {task_done}')
    for task_pending in pending:
        logger.info(f'pending: {task_pending}')

    assert_fn(
        trader_tasks_started_later=broker.trader_tasks_started_later,
        trader_task=trader_task,
        position_synthesizer_task=position_synthesizer_task,
        start_new_trader_task=start_new_trader_task,
        observer_task=observer_task
    )


if __name__ == '__main__':
    _p = ArgumentParser()
    _p.add_argument('--common', action='store_true')
    _p.add_argument('--retry', action='store_true')
    _p.add_argument('--ood', action='store_true')
    _args = _p.parse_args()

    _logger = get_logger(name='asyncio', stream=sys.stdout,  # level='DEBUG',
                         _format='%(asctime)s: %(levelname)s:%(message)s')

    if _args.common:
        asyncio.run(main(logger=_logger, strategy_classes=[StrategyCommon], assert_fn=assert_general), debug=True)
    elif _args.retry:
        asyncio.run(main(logger=_logger, strategy_classes=[StrategyRetry], assert_fn=assert_general), debug=True)
    elif _args.ood:
        asyncio.run(main(logger=_logger, strategy_classes=[StrategyOOD], assert_fn=assert_ood), debug=True)
    else:
        raise Exception('Unexpected argument')
