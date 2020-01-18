import sqlite3
import unittest
from decimal import Decimal
from typing import AsyncIterable, AsyncIterator

import numpy as np
import sys

from trade.execution.model import Execution
from trade.execution.stream.tests.test_sqlite import e1, e2
from trade.executionwriter.sqlite import SqliteExecutionWriter, AbstractConnection
from trade.log import get_logger
from trade.model import Symbol


class SqliteExecutionWriterTestCase(unittest.IsolatedAsyncioTestCase):
    class InMemoryConnection(AbstractConnection):

        def __init__(self):
            self._con = sqlite3.connect(':memory:')

        def open_as_temporary(self) -> sqlite3.Connection:
            return self._con

        def close(self):
            return 'Dummy path'

        def get(self):
            return self._con

    async def test_write(self):
        class Iterable(AsyncIterable[Execution]):
            async def __aiter__(self) -> AsyncIterator[Execution]:
                yield e1
                yield e2
                yield Execution(symbol=Symbol.FXBTCJPY, _id=3,
                                timestamp=np.datetime64('2019-07-07T08:59:58.877569400'),
                                side=None, price=Decimal('100'), size=Decimal('0.01'),
                                buy_child_order_acceptance_id='JRF20190707-085958-692751',
                                sell_child_order_acceptance_id='JRF20190707-085958-403844')
                yield Execution(symbol=Symbol.FXBTCJPY, _id=None,
                                timestamp=np.datetime64('2019-07-07T08:59:58.877569400'),
                                side=None, price=Decimal('100'), size=Decimal('0.01'),
                                buy_child_order_acceptance_id='JRF20190707-085958-692751',
                                sell_child_order_acceptance_id='JRF20190707-085958-403844')

        connection = self.InMemoryConnection()

        writer = SqliteExecutionWriter(
            logger=get_logger(self.__class__.__name__, stream=sys.stdout),
            connection=connection,
            records_rotation=8,
            records_insertion=4,
        )
        await writer.write(iterable=Iterable())

        actual = list()
        for row in connection.get().cursor().execute('SELECT * FROM executions'):
            actual.append(row)

        self.assertEqual(4, len(actual))
        self.assertEqual(
            ('FXBTCJPY', 1, '2019-07-07T08:59:58.877569400', 'BUY', 100, 0.01, 'JRF20190707-085958-692751',
             'JRF20190707-085958-403844', None, None, None, None, None, None, None, None, None, None),
            actual[0]
        )
        self.assertEqual(
            ('FXBTCJPY', 2, '2019-07-07T08:59:59.877569400', 'SELL', 10, 1.23, 'JRF20190707-085958-692752',
             'JRF20190707-085958-403845', 0.11111111111111112, -1000000001, 'BTCJPY', 3,
             '2019-07-07T09:00:00.877569401', 'SELL', 9, 1.1, 'JRF20190707-085958-692753', 'JRF20190707-085958-403846'),
            actual[1]
        )
        self.assertEqual(
            ('FXBTCJPY', 3, '2019-07-07T08:59:58.877569400', '', 100, 0.01, 'JRF20190707-085958-692751',
             'JRF20190707-085958-403844', None, None, None, None, None, None, None, None, None, None),
            actual[2]
        )
        self.assertEqual(
            ('FXBTCJPY', None, '2019-07-07T08:59:58.877569400', '', 100, 0.01, 'JRF20190707-085958-692751',
             'JRF20190707-085958-403844', None, None, None, None, None, None, None, None, None, None),
            actual[3]
        )

    async def test_write_lt_insertion(self):
        class Iterable(AsyncIterable[Execution]):
            async def __aiter__(self) -> AsyncIterator[Execution]:
                yield e1
                yield e2
                yield Execution(symbol=Symbol.FXBTCJPY, _id=3,
                                timestamp=np.datetime64('2019-07-07T08:59:58.877569400'),
                                side=None, price=Decimal('100'), size=Decimal('0.01'),
                                buy_child_order_acceptance_id='JRF20190707-085958-692751',
                                sell_child_order_acceptance_id='JRF20190707-085958-403844')
                yield Execution(symbol=Symbol.FXBTCJPY, _id=None,
                                timestamp=np.datetime64('2019-07-07T08:59:58.877569400'),
                                side=None, price=Decimal('100'), size=Decimal('0.01'),
                                buy_child_order_acceptance_id='JRF20190707-085958-692751',
                                sell_child_order_acceptance_id='JRF20190707-085958-403844')

        connection = self.InMemoryConnection()

        writer = SqliteExecutionWriter(
            logger=get_logger(self.__class__.__name__, stream=sys.stdout),
            connection=connection,
            records_rotation=10,
            records_insertion=5,
        )
        await writer.write(iterable=Iterable())

        actual = list()
        for row in connection.get().cursor().execute('SELECT * FROM executions'):
            actual.append(row)

        self.assertEqual(0, len(actual))


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(SqliteExecutionWriterTestCase))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='test_suite', verbosity=2, buffer=True)
