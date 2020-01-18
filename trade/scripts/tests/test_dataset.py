import os
import shutil
import sqlite3
import unittest

from trade.log import get_logger
from trade.scripts.dataset import Updater
from trade.tests import Path


class UpdaterTestCase(unittest.TestCase):

    def setUp(self):
        self._path = Path(os.path.dirname(os.path.abspath(__file__)))

    def test_reduce_sqlite3_new_prices(self):
        symbol_dir = self._path.abs('test_dataset/newprices/FXBTCJPY')
        process_dir = os.path.join(symbol_dir, 'sqlite3-reduced-newprices1min')
        if os.path.exists(process_dir):
            shutil.rmtree(process_dir)

        try:
            logger = get_logger(__name__)
            updater = Updater(
                logger=logger, symbol_dir=symbol_dir, symbol='FXBTCJPY', exchange='bitflyer', timeunit='1minute',
                append_after=None
            )
            updater.reduce_sqlite3_new_prices('sqlite3', 'sqlite3-reduced-newprices1min')

            destination = os.listdir(process_dir)
            self.assertEqual(1, len(destination))

            with open(self._path.abs('test_reduce_sqlite3_new_prices.expected')) as fd:
                expected = fd.readlines()

                actual = list()
                conn = sqlite3.connect(os.path.join(process_dir, destination[0]))
                for row in conn.execute('SELECT * FROM executions ORDER BY id'):
                    actual.append(row)

                self.assertEqual(len(expected), len(actual))
                for n, e in enumerate(expected):
                    self.assertEqual(str(e.strip()), str(actual[n]))

                conn.close()
        finally:
            if os.path.exists(process_dir):
                shutil.rmtree(process_dir)

    def test_reduce_sqlite3_ohlc_4variables(self):
        symbol_dir = self._path.abs('test_dataset/ohlc/FXBTCJPY')
        process_dir = os.path.join(symbol_dir, 'sqlite3-reduced-ohlc1min')
        if os.path.exists(process_dir):
            shutil.rmtree(process_dir)

        try:
            logger = get_logger(__name__)
            updater = Updater(
                logger=logger, symbol_dir=symbol_dir, symbol='FXBTCJPY', exchange='bitflyer', timeunit='1minute',
                append_after=None
            )
            updater.reduce_sqlite3_ohlc_4variables('sqlite3-reduced-newprices1min', 'sqlite3-reduced-ohlc1min')

            destination = os.listdir(process_dir)
            self.assertEqual(1, len(destination))

            with open(self._path.abs('test_reduce_sqlite3_ohlc_4variables.expected')) as fd:
                expected = fd.readlines()

                actual = list()
                conn = sqlite3.connect(os.path.join(process_dir, destination[0]))
                for row in conn.execute('SELECT * FROM executions ORDER BY id'):
                    actual.append(row)

                self.assertEqual(len(expected), len(actual))
                for n, e in enumerate(expected):
                    self.assertEqual(str(e.strip()), str(actual[n]))

                conn.close()
        finally:
            if os.path.exists(process_dir):
                shutil.rmtree(process_dir)


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(UpdaterTestCase))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='test_suite', verbosity=2, buffer=True)
