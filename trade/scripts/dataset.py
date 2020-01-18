import asyncio
from argparse import ArgumentParser, Namespace
from logging import Logger
from typing import AsyncIterable, List, Optional, Dict, Any

import numpy as np
import sys

from trade.execution.model import Execution
from trade.execution.stream.adapter.filter import DropWhileStream, NewPricesStream, OHLCStream
from trade.execution.stream.adapter.sync import SynchronizedStream
from trade.execution.stream.chain import ChainedStream
from trade.execution.stream.s3 import S3Stream, list_s3_keys, build_s3_key_prefix
from trade.execution.stream.sqlite import list_sqlite_connections, SqliteStreamReader
from trade.executionwriter.sqlite import Connection, SqliteExecutionWriter
from trade.model import Exchange, Symbol


async def setup_sqlite_wrapper(logger: Logger, args: Namespace):
    args: Dict[str, Any] = vars(args)

    del args['func']

    if not args['datetime_from'] or args['datetime_from'] == "''":
        args['datetime_from'] = np.datetime64('NaT')

    args['symbol'] = Symbol(args['symbol'])
    args['exchange'] = Exchange(args['exchange'])

    await setup_sqlite(logger=logger, **args)


async def setup_sqlite_synchronized_reduced_newprices_wrapper(logger: Logger, args: Namespace):
    args: Dict[str, Any] = vars(args)

    del args['func']

    if not args['datetime_from'] or args['datetime_from'] == "''":
        args['datetime_from'] = np.datetime64('NaT')

    await setup_sqlite_synchronized_reduced_newprices(logger=logger, **args)


async def setup_sqlite_synchronized_reduced_ohlc_wrapper(logger: Logger, args: Namespace):
    args: Dict[str, Any] = vars(args)

    del args['func']

    if not args['datetime_from'] or args['datetime_from'] == "''":
        args['datetime_from'] = np.datetime64('NaT')

    await setup_sqlite_synchronized_reduced_ohlc(logger=logger, **args)


async def setup_sqlite(
        logger: Logger,
        s3_bucket: str,
        symbol: Symbol,
        exchange: Exchange,
        channel: str,
        version: int,
        s3_key_prefix_year: Optional[int],
        s3_key_prefix_month: Optional[int],
        s3_key_prefix_day: Optional[int],
        destination_directory: str,
        datetime_from: np.datetime64,
):
    if np.isnat(datetime_from):
        datetime_from: np.datetime64 = np.datetime64(datetime_from, 'ns', utc=True)
    logger.info(f'datetime_from: {datetime_from}')

    s3_key_prefix = build_s3_key_prefix(
        logger=_logger,
        symbol=symbol,
        exchange=exchange,
        channel=channel,
        version=version,
        year=s3_key_prefix_year,
        month=s3_key_prefix_month,
        day=s3_key_prefix_day,
    )
    connection = Connection(basedir=destination_directory, exchange=exchange)
    writer = SqliteExecutionWriter(logger=logger, connection=connection)

    for s3_key in list_s3_keys(
            logger=logger, bucket=s3_bucket, s3_key_prefix=s3_key_prefix, datetime_from=datetime_from
    ):
        s3_stream = S3Stream(logger, bucket=s3_bucket, key=s3_key, symbol=symbol)
        await writer.write(iterable=s3_stream)


async def setup_sqlite_synchronized_reduced_newprices(
        logger: Logger,
        time_window: str,
        primary_directory: str,
        secondary_directory: str,
        destination_directory: str,
        datetime_from: np.datetime64,
):
    if np.isnat(datetime_from):
        datetime_from: np.datetime64 = np.datetime64(datetime_from, 'ns', utc=True)
    logger.info(f'datetime_from: {datetime_from}')

    primary_iterables: List[AsyncIterable[Execution]] = list()
    for primary_con in list_sqlite_connections(path=primary_directory, datetime_from=datetime_from):
        primary_iterables.append(SqliteStreamReader(logger=logger, connection=primary_con))

    secondary_iterables: List[AsyncIterable[Execution]] = list()
    for secondary_con in list_sqlite_connections(path=secondary_directory, datetime_from=datetime_from):
        secondary_iterables.append(SqliteStreamReader(logger=logger, connection=secondary_con))

    primary_stream: AsyncIterable[Execution] = NewPricesStream(
        logger=logger, time_window=time_window,

        upstream=DropWhileStream(
            logger=logger, predicate=lambda e: False,  # TODO: 必要なときにフィルタリングを実装

            upstream=ChainedStream(
                logger=logger, upstreams=primary_iterables
            )
        )
    )
    secondary_stream: AsyncIterable[Execution] = DropWhileStream(
        logger=logger, predicate=lambda e: False,  # TODO: 必要なときにフィルタリングを実装

        upstream=ChainedStream(
            logger=logger, upstreams=secondary_iterables
        )
    )

    write_con = Connection(basedir=destination_directory, exchange=Exchange.bitFlyer)

    await SqliteExecutionWriter(logger=logger, connection=write_con).write(
        SynchronizedStream(
            logger=logger, primary_iterable=primary_stream, secondary_iterable=secondary_stream
        )
    )
    write_con.close()


async def setup_sqlite_synchronized_reduced_ohlc(
        logger: Logger,
        time_window: str,
        source_directory: str,
        destination_directory: str,
        datetime_from: np.datetime64,
):
    if np.isnat(datetime_from):
        datetime_from: np.datetime64 = np.datetime64(datetime_from, 'ns', utc=True)
    logger.info(f'datetime_from: {datetime_from}')

    source_iterables: List[AsyncIterable[Execution]] = list()
    for con in list_sqlite_connections(path=source_directory, datetime_from=datetime_from):
        source_iterables.append(SqliteStreamReader(logger, connection=con))

    reduced_stream: AsyncIterable[Execution] = OHLCStream(
        logger=logger, time_window=time_window,

        upstream=DropWhileStream(
            logger=logger, predicate=lambda e: False,  # TODO: 必要なときにフィルタリングを実装

            upstream=ChainedStream(
                logger=logger, upstreams=source_iterables
            )
        )
    )

    write_con = Connection(basedir=destination_directory, exchange=Exchange.bitFlyer)

    await SqliteExecutionWriter(logger=logger, connection=write_con).write(
        iterable=reduced_stream
    )
    write_con.close()


if __name__ == '__main__':
    from trade.log import get_logger

    _logger = get_logger(__name__, stream=sys.stdout, _format='%(asctime)s:%(levelname)s:%(message)s')

    _p = ArgumentParser()
    _subparsers = _p.add_subparsers()

    _p_setup_sqlite = _subparsers.add_parser('setup-sqlite')
    _p_setup_sqlite.add_argument('--s3-bucket')
    _p_setup_sqlite.add_argument('--symbol')
    _p_setup_sqlite.add_argument('--exchange')
    _p_setup_sqlite.add_argument('--channel')
    _p_setup_sqlite.add_argument('--version')
    _p_setup_sqlite.add_argument('--s3-key-prefix-year', default=None)
    _p_setup_sqlite.add_argument('--s3-key-prefix-month', default=None)
    _p_setup_sqlite.add_argument('--s3-key-prefix-day', default=None)
    _p_setup_sqlite.add_argument('--datetime-from', default=None)
    _p_setup_sqlite.add_argument('--destination-directory')
    _p_setup_sqlite.set_defaults(func=setup_sqlite_wrapper)

    _p_setup_reduced_newprices = _subparsers.add_parser('setup-reduced-newprices')
    _p_setup_reduced_newprices.add_argument('--time-window')
    _p_setup_reduced_newprices.add_argument('--primary-directory')
    _p_setup_reduced_newprices.add_argument('--secondary-directory')
    _p_setup_reduced_newprices.add_argument('--destination-directory')
    _p_setup_reduced_newprices.add_argument('--datetime-from', default=None)
    _p_setup_reduced_newprices.set_defaults(func=setup_sqlite_synchronized_reduced_newprices_wrapper)

    _p_setup_reduced_ohlc = _subparsers.add_parser('setup-reduced-ohlc')
    _p_setup_reduced_ohlc.add_argument('--time-window')
    _p_setup_reduced_ohlc.add_argument('--source-directory')
    _p_setup_reduced_ohlc.add_argument('--destination-directory')
    _p_setup_reduced_ohlc.add_argument('--datetime-from', default=None)
    _p_setup_reduced_ohlc.set_defaults(func=setup_sqlite_synchronized_reduced_ohlc_wrapper)

    _args = _p.parse_args()
    asyncio.run(_args.func(logger=_logger, args=_args))
