import os
import sqlite3
from decimal import Decimal
from logging import Logger
from typing import Iterator, AsyncIterable, AsyncIterator

import numpy as np

from trade.execution.model import Execution, SynchronizedExecution
from trade.execution import Chunk
from trade.model import Symbol, Exchange, normalize_exchange_name
from trade.side import Side


class SqliteStreamReader(AsyncIterable[Execution]):
    """
    SQLiteデータベースを源とする、Executionストリーム
    """

    def __init__(self, logger: Logger, connection: sqlite3.Connection):
        self._logger = logger
        self._connection = connection
        self._connection.row_factory = sqlite3.Row

    async def __aiter__(self) -> AsyncIterator[Execution]:
        with self._connection:
            for row in self._connection.execute('SELECT * FROM executions WHERE id IS NOT NULL ORDER BY id'):
                yield Execution(
                    symbol=Symbol(row['symbol']),
                    _id=row['id'],
                    timestamp=np.datetime64(row['timestamp'].rstrip('Z'), 'ns', utc=True),
                    side=row['side'] and Side(row['side']) or Side.NOTHING,
                    price=Decimal(str(row['price'])),
                    size=Decimal(str(row['size'])),
                    buy_child_order_acceptance_id=row['buy_child_order_acceptance_id'],
                    sell_child_order_acceptance_id=row['sell_child_order_acceptance_id'],
                    synchronized_execution_price_deviation=(
                            row['synchronized_execution_price_deviation']
                            and Decimal(str(row['synchronized_execution_price_deviation']))
                    ),
                    synchronized_execution_time_delta=(
                            row['synchronized_execution_time_delta']
                            and np.timedelta64(row['synchronized_execution_time_delta'], 'ns')
                    ),
                    synchronized_execution=SynchronizedExecution(
                        symbol=row['synchronized_symbol'] and Symbol(row['synchronized_symbol']),
                        _id=row['synchronized_id'] and row['synchronized_id'],
                        timestamp=(
                                row['synchronized_timestamp']
                                and np.datetime64(row['synchronized_timestamp'].rstrip('Z'), 'ns', utc=True)
                        ),
                        side=row['synchronized_side'] and Side(row['synchronized_side']),
                        price=row['synchronized_price'] and Decimal(str(row['synchronized_price'])),
                        size=row['synchronized_size'] and Decimal(str(row['synchronized_size'])),
                        buy_child_order_acceptance_id=(
                                row['synchronized_buy_child_order_acceptance_id']
                                and row['synchronized_buy_child_order_acceptance_id']
                        ),
                        sell_child_order_acceptance_id=(
                                row['synchronized_sell_child_order_acceptance_id']
                                and row['synchronized_sell_child_order_acceptance_id']
                        ),
                    )
                )


class FileName:
    """
    Filename parser/unparser for SQLite database.

    Filename is like to ...
    <bitFlyer>_<FXBTCJPY>_<1146957467>-<2019-07-07T085958.877569400>_<1147008386>-<2019-07-07T100259.385583600>.sqlite3
    """

    @staticmethod
    def parse(filename) -> Chunk:
        e = filename.split('_')
        exchange = Exchange(normalize_exchange_name(e[0]))
        symbol = Symbol(e[1])
        first_id, first_datetime = e[2].split('-', maxsplit=1)
        first_id = int(first_id)
        first_datetime = np.datetime64(FileName.decode_safe_filename(first_datetime), 'ns')
        last_id, last_datetime = e[3].split('-', maxsplit=1)
        last_id = int(last_id)
        last_datetime = np.datetime64(FileName.decode_safe_filename(last_datetime.replace('.sqlite3', '')), 'ns')
        return Chunk(exchange=exchange, symbol=symbol, first_id=first_id, first_datetime=first_datetime,
                     last_id=last_id, last_datetime=last_datetime)

    @staticmethod
    def unparse(c: Chunk) -> str:
        return f'{c.exchange.value}_{c.symbol.value}' \
               f'_{c.first_id}-{FileName.encode_safe_filename(str(c.first_datetime))}' \
               f'_{c.last_id}-{FileName.encode_safe_filename(str(c.last_datetime))}.sqlite3'

    @staticmethod
    def encode_safe_filename(iso8601_string: str) -> str:
        return iso8601_string.replace(':', '')

    @staticmethod
    def decode_safe_filename(safe_datetime_string: str) -> str:
        date, time = safe_datetime_string.split('T')

        def _inject(_time: str):
            time_parts = list()
            for n, v in enumerate(_time):
                time_parts.append(v)
                if n % 2 == 1:
                    time_parts.append(':')
            return ''.join(time_parts).rstrip(':')

        if ',' in time:
            time, appendix = time.split(',')
            time = _inject(time)
            return ''.join([date, 'T', time, ',', appendix])
        else:
            time, appendix = time.split('.')
            time = _inject(time)
            return ''.join([date, 'T', time, '.', appendix])


def list_sqlite_connections(path: str, datetime_from: np.datetime64 = None) -> Iterator[sqlite3.Connection]:
    """
    SQLiteデータベース接続のイテレータを返します。
    :param path: データベースファイルが含まれるディレクトリのパス
    :param datetime_from: 指定された場合、この日時以降のデータベース接続のみ返されます
    :return: SQLiteデータベース接続のイテレータ。Execution idの昇順にソートされています。
    """
    # When path is a file
    if os.path.isfile(path):
        first_dt: np.datetime64 = FileName.parse(os.path.basename(path)).first_datetime
        if datetime_from and first_dt < datetime_from:
            return

        yield sqlite3.Connection(path)
        return

    # When path is a directory

    indices = dict()
    for filename in os.listdir(path):
        first_dt: np.datetime64 = FileName.parse(filename).first_datetime
        if datetime_from and first_dt < datetime_from:
            continue

        e = filename.split('-', maxsplit=1)
        first_id = int(e[0].split('_')[2])
        indices[first_id] = '-'.join(e)

    for _id in sorted(indices.keys()):
        filename = indices[_id]
        yield sqlite3.Connection(os.path.join(path, filename))
