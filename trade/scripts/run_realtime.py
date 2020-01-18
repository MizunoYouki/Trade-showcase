import asyncio
import sys
from argparse import ArgumentParser
from asyncio import Task
from decimal import Decimal
from logging import Logger
from typing import AsyncIterable, Union

from aiohttp import ClientSession

from trade.broker.declarative.bitflyer import BitflyerBroker, parse_credentials
from trade.broker.declarative.model import NormalizedPositions
from trade.broker.declarative.queue import LifoQueue as LifoQueueClearable
from trade.broker.httpclient import HTTPClient
from trade.execution.model import encode_bitflyer_channel, Execution, SwitchedToRealtime
from trade.execution.stream.realtime import RealtimeWebSocketStream
from trade.log import get_logger
from trade.strategies import Strategies
from trade.strategies.stub import StrategiesStub
from trade.strategy.stub import RandomDotenStrategy


async def execution_feeder(reader: AsyncIterable[Union[Execution, SwitchedToRealtime]],
                           execution_queue: 'asyncio.Queue[Union[Execution, SwitchedToRealtime]]'):
    async for execution in reader:
        execution_queue.put_nowait(execution)


async def run_realtime_random(logger: Logger,
                              size: Decimal,
                              websocket_reader: AsyncIterable[Union[Execution, SwitchedToRealtime]],
                              time_window: str):
    execution_queue: 'asyncio.Queue[Execution]' = asyncio.Queue()
    positions_queue: 'LifoQueueClearable[NormalizedPositions]' = LifoQueueClearable(logger)
    api_key, api_secret = parse_credentials()

    strategies: Strategies = StrategiesStub(
        logger,
        strategies=[
            RandomDotenStrategy(logger, time_window=time_window)
        ],
        size=size
    )

    execution_feeder_task: Task = asyncio.create_task(
        execution_feeder(reader=websocket_reader, execution_queue=execution_queue)
    )
    positions_distributor_task: Task = asyncio.create_task(
        strategies.positions_distributor(
            execution_queue=execution_queue,
            positions_queue=positions_queue
        )
    )
    async with ClientSession() as client_session:
        broker: BitflyerBroker = await BitflyerBroker.create_broker(
            positions_queue=positions_queue,
            logger=logger,
            http_client=HTTPClient(
                logger, client_session=client_session, time_wait_retrying=1, time_wait_429_suspends=300
            ),
            api_key=api_key,
            api_secret=api_secret,
            delay=0.7,
        )

        trader_task: Task = asyncio.create_task(broker.trader())

        done, pending = await asyncio.wait([
            execution_feeder_task,
            positions_distributor_task,
            asyncio.create_task(broker.start_new_trader(trader_task=trader_task)),
            asyncio.create_task(broker.observer()),
            trader_task,
        ], return_when=asyncio.ALL_COMPLETED)

        logger.info(f'done: {done}')
        logger.info(f'pending: {pending}')


if __name__ == '__main__':
    _p = ArgumentParser()
    _p.add_argument('--strategy')
    _p.add_argument('--size')
    _p.add_argument('--websocket-uri')
    _p.add_argument('--time-window')
    _args = _p.parse_args()

    _logger = get_logger(__name__, _format='%(asctime)s:%(levelname)s:%(message)s', stream=sys.stdout, level='DEBUG')
    _websocket_reader = RealtimeWebSocketStream(
        logger=_logger,
        uri=_args.websocket_uri,
        symbol_resolver=encode_bitflyer_channel,
        execution_encoder=Execution.encode_bitflyer_response
    )

    if _args.strategy == 'random-doten':
        asyncio.run(
            run_realtime_random(
                logger=_logger,
                size=Decimal(str(_args.size)),
                websocket_reader=_websocket_reader,
                time_window=_args.time_window
            ),
            debug=True
        )
    else:
        _p.error(_p.format_usage())

