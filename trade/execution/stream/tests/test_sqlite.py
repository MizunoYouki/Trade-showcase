import sqlite3
import unittest
from decimal import Decimal

import numpy as np

from trade.execution.model import Execution, SynchronizedExecution
from trade.execution.stream.sqlite import SqliteStreamReader, FileName
from trade.log import get_logger
from trade.model import Symbol
from trade.side import Side

e1 = Execution(symbol=Symbol.FXBTCJPY, _id=1, timestamp=np.datetime64('2019-07-07T08:59:58.877569400'),
               side=Side.BUY, price=Decimal('100'), size=Decimal('0.01'),
               buy_child_order_acceptance_id='JRF20190707-085958-692751',
               sell_child_order_acceptance_id='JRF20190707-085958-403844', timeunit_if_ohlc_from=None,
               synchronized_execution_price_deviation=None, synchronized_execution_time_delta=None,
               synchronized_execution=SynchronizedExecution())
e2 = Execution(symbol=Symbol.FXBTCJPY, _id=2, timestamp=np.datetime64('2019-07-07T08:59:59.877569400'),
               side=Side.SELL, price=Decimal('10'), size=Decimal('1.23'),
               buy_child_order_acceptance_id='JRF20190707-085958-692752',
               sell_child_order_acceptance_id='JRF20190707-085958-403845',
               timeunit_if_ohlc_from=None,
               synchronized_execution_price_deviation=Decimal('0.11111111111111112'),
               synchronized_execution_time_delta=np.timedelta64(-1000000001, 'ns'),
               synchronized_execution=SynchronizedExecution(
                   symbol=Symbol.BTCJPY, _id=3,
                   timestamp=np.datetime64('2019-07-07T09:00:00.877569401'),
                   side=Side.SELL, price=Decimal('9'), size=Decimal('1.1'),
                   buy_child_order_acceptance_id='JRF20190707-085958-692753',
                   sell_child_order_acceptance_id='JRF20190707-085958-403846')
               )


class SqliteStreamTestCase(unittest.IsolatedAsyncioTestCase):

    async def test_aiter(self):
        con = sqlite3.connect(':memory:')
        cur = con.cursor()
        cur.execute('CREATE TABLE executions ('
                    'symbol TEXT NOT NULL, '
                    'id INTEGER NOT NULL, '
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
        cur.execute(
            'INSERT INTO executions ('
            'symbol, id, timestamp, side, price, size, buy_child_order_acceptance_id,  sell_child_order_acceptance_id'
            ') VALUES ('
            '"FXBTCJPY", 1, "2019-07-07T08:59:58.8775694", "BUY", 100, 0.01, '
            '"JRF20190707-085958-692751", "JRF20190707-085958-403844")')
        cur.execute(
            'INSERT INTO executions ('
            'symbol, id, timestamp, side, price, size, buy_child_order_acceptance_id,  sell_child_order_acceptance_id, '
            'synchronized_execution_price_deviation, '
            'synchronized_execution_time_delta, '
            'synchronized_symbol, '
            'synchronized_id, '
            'synchronized_timestamp, '
            'synchronized_side, '
            'synchronized_price, '
            'synchronized_size, '
            'synchronized_buy_child_order_acceptance_id, '
            'synchronized_sell_child_order_acceptance_id'
            ') VALUES ('
            '"FXBTCJPY", 2, "2019-07-07T08:59:59.8775694", "SELL", 10, 1.23, '
            '"JRF20190707-085958-692752", "JRF20190707-085958-403845", '
            '0.11111111111111112, -1000000001, '
            '"BTCJPY", 3, "2019-07-07T09:00:00.877569401", "SELL", 9, 1.1, '
            '"JRF20190707-085958-692753", "JRF20190707-085958-403846")'
        )

        actual = list()
        reader = SqliteStreamReader(
            logger=get_logger(self.__class__.__name__),
            connection=con
        )
        async for execution in reader:
            actual.append(execution)

        self.assertEqual(2, len(actual))
        self.assertEqual(e1, actual[0])
        self.assertEqual(e2, actual[1])


class FileNameTestCase(unittest.TestCase):

    def test_decode_safe_filename(self):
        safe_datetime_string = '2019-07-07T100259.385583600'
        self.assertEqual('2019-07-07T10:02:59.385583600', FileName.decode_safe_filename(safe_datetime_string))


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(SqliteStreamTestCase))
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(FileNameTestCase))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='test_suite', verbosity=2, buffer=True)
