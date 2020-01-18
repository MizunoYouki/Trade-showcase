import asyncio
import sys
from argparse import ArgumentParser
from decimal import Decimal
from logging import Logger
from typing import AsyncIterable

from trade.broker.stub import stub_broker
from trade.execution.model import Execution
from trade.execution.stream.chain import ChainedStream
from trade.execution.stream.sqlite import SqliteStreamReader, list_sqlite_connections
from trade.log import get_logger
from trade.strategy.stub import RandomDotenStrategy


def playback_random_doten(logger: Logger, reader: AsyncIterable[Execution]):
    time_window = '30minute'
    losscut = Decimal('-8000')

    asyncio.run(stub_broker(
        logger,
        reader=reader,
        strategy=RandomDotenStrategy(
            logger,
            time_window=time_window
        ),
        losscut=losscut
    ))


if __name__ == '__main__':
    _p = ArgumentParser()
    _p.add_argument('--strategy')
    _p.add_argument('--sqlite-basedir')
    _args = _p.parse_args()

    _logger = get_logger(__name__, _format='%(asctime)s:%(levelname)s:%(message)s', stream=sys.stdout)
    _sqlite_connections = list_sqlite_connections(path=_args.sqlite_basedir, datetime_from=None)
    _reader = ChainedStream(
        _logger,
        upstreams=[SqliteStreamReader(_logger, connection=c) for c in _sqlite_connections]
    )

    if _args.strategy == 'random':
        playback_random_doten(_logger, _reader)
    else:
        _p.error(_p.format_usage())
