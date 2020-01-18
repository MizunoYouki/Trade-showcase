import os
import sqlite3
from abc import abstractmethod
from logging import Logger
from typing import AsyncIterable, Iterable, Optional, List, Tuple

import numpy as np

from trade.execution import Chunk
from trade.execution.model import Execution
from trade.execution.stream.sqlite import FileName
from trade.model import Exchange, Symbol


class AbstractConnection:

    @abstractmethod
    def open_as_temporary(self) -> sqlite3.Connection:
        pass

    @abstractmethod
    def close(self) -> str:
        pass


class Connection(AbstractConnection):

    def __init__(self, basedir: str, exchange: Exchange):
        self._basedir = basedir
        if not os.path.exists(self._basedir):
            os.makedirs(self._basedir)

        self._exchange = exchange
        self._temp_path = os.path.join(self._basedir, 'temp.sqlite3')

        self._con: Optional[sqlite3.Connection] = None

    def open_as_temporary(self) -> sqlite3.Connection:
        self._con: Optional[sqlite3.Connection] = sqlite3.Connection(self._temp_path)
        self._con.row_factory = sqlite3.Row
        return self._con

    def close(self) -> str:
        cur = self._con.cursor()

        first = cur.execute('SELECT * FROM executions WHERE id IS NOT NULL ORDER BY id LIMIT 1').fetchone()
        last = cur.execute('SELECT * FROM executions WHERE id IS NOT NULL ORDER BY id DESC LIMIT 1').fetchone()

        cur.close()
        self._con.commit()
        self._con.close()

        chunk = Chunk(
            exchange=self._exchange, symbol=Symbol(first['symbol']),
            first_id=first['id'], first_datetime=np.datetime64(first['timestamp'], 'ns'),
            last_id=last['id'], last_datetime=np.datetime64(last['timestamp'], 'ns')
        )
        to_path = os.path.join(self._basedir, FileName.unparse(chunk))

        os.rename(self._temp_path, to_path)
        return to_path


class SqliteExecutionWriter:

    def __init__(self, logger: Logger,
                 connection: AbstractConnection,
                 records_rotation: int = 1_000_000,
                 records_insertion: int = 100_000):
        self._logger = logger
        self._connection = connection
        self._n_records_rotation = records_rotation
        self._n_records_insertion = records_insertion
        self._buf: List[Tuple] = list()
        self._n = 0
        self._cursor = self._connection.open_as_temporary().cursor()

    def _execute_many(self, _buf: Iterable[Iterable]):
        self._cursor.executemany(
            'INSERT INTO executions '
            'VALUES ('
            '?,'
            '?,'
            '?,'
            '?,'
            '?,'
            '?,'
            '?,'
            '?,'
            '?,'
            '?,'
            '?,'
            '?,'
            '?,'
            '?,'
            '?,'
            '?,'
            '?,'
            '?)', _buf
        )

    async def write(self, iterable: AsyncIterable[Execution]):
        """
        self._records_insertion のレコード数毎にデータベースファイルへ書き出し、self._records_rotationに達するとファイルを
        分割します。
        """
        self._create_table_if_not_exists(self._cursor)

        self._logger.info(f'new iterable, n: {self._n}, len(buf): {len(self._buf)}')

        async for e in iterable:
            self._buf.append((
                e.symbol.value,
                (e._id and e._id or None),
                str(e.timestamp),
                (e.side and e.side.value or ''),
                str(e.price),
                str(e.size),
                e.buy_child_order_acceptance_id,
                e.sell_child_order_acceptance_id,
                e.synchronized_execution_price_deviation and str(e.synchronized_execution_price_deviation) or None,
                e.synchronized_execution_time_delta and e.synchronized_execution_time_delta.item() or None,
                (e.synchronized_execution and e.synchronized_execution.symbol
                 and e.synchronized_execution.symbol.value or None),
                (e.synchronized_execution and e.synchronized_execution._id
                 and e.synchronized_execution._id or None),
                (e.synchronized_execution and e.synchronized_execution.timestamp
                 and str(e.synchronized_execution.timestamp) or None),
                (e.synchronized_execution and e.synchronized_execution.side
                 and e.synchronized_execution.side.value or None),
                (e.synchronized_execution and e.synchronized_execution.price
                 and str(e.synchronized_execution.price) or None),
                (e.synchronized_execution and e.synchronized_execution.size
                 and str(e.synchronized_execution.size) or None),
                (e.synchronized_execution and e.synchronized_execution.buy_child_order_acceptance_id
                 and e.synchronized_execution.buy_child_order_acceptance_id or None),
                (e.synchronized_execution and e.synchronized_execution.sell_child_order_acceptance_id
                 and e.synchronized_execution.sell_child_order_acceptance_id or None),
            ))
            self._n += 1

            if self._n % self._n_records_insertion == 0:
                self._execute_many(self._buf)
                self._logger.info(
                    f'inserted {len(self._buf)} buffer records'
                    f', subtotal n: {int(self._n_records_insertion * (self._n / self._n_records_insertion))} records'
                )
                self._buf.clear()

                if self._n == self._n_records_rotation:
                    path = self._connection.close()
                    self._logger.info(f'rotated, n: {self._n}, filename: {os.path.basename(path)}')

                    self._n = 0
                    self._cursor = self._connection.open_as_temporary().cursor()
                    self._create_table_if_not_exists(self._cursor)

        self._logger.info(f'end of iterable, n: {self._n}, len(buf): {len(self._buf)}')

    def _create_table_if_not_exists(self, cur: sqlite3.Cursor):
        cur.execute('CREATE TABLE IF NOT EXISTS executions ('
                    'symbol TEXT NOT NULL, '
                    'id INTEGER, '
                    'timestamp TIMESTAMP NOT NULL, '
                    'side TEXT, '
                    'price INTEGER NOT NULL, '
                    'size REAL, '
                    'buy_child_order_acceptance_id TEXT, '
                    'sell_child_order_acceptance_id TEXT, '
                    'synchronized_execution_price_deviation REAL, '
                    'synchronized_execution_time_delta INTEGER, '
                    'synchronized_symbol TEXT, '
                    'synchronized_id INTEGER, '
                    'synchronized_timestamp TIMESTAMP, '
                    'synchronized_side TEXT, '
                    'synchronized_price INTEGER, '
                    'synchronized_size REAL, '
                    'synchronized_buy_child_order_acceptance_id TEXT, '
                    'synchronized_sell_child_order_acceptance_id TEXT)')
