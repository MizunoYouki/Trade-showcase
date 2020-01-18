import unittest
from datetime import datetime
from decimal import Decimal
from functools import partial
from typing import AsyncIterable, AsyncIterator

import numpy as np

from trade.execution.model import Execution
from trade.execution.stream.adapter.filter import OHLCStream, DropWhileStream, NewPricesStream
from trade.log import get_logger
from trade.model import Symbol
from trade.test_helper import make_execution, build_iterator

_me = partial(make_execution, symbol=Symbol.FXBTCJPY)


class DropWhileStreamTestCase(unittest.IsolatedAsyncioTestCase):

    async def test_aiter_empty(self):
        class _Iterator(AsyncIterable[Execution]):
            def __aiter__(self):
                return

        reader = DropWhileStream(
            logger=get_logger(self.test_aiter_empty.__name__),
            upstream=_Iterator(),
            predicate=lambda e: False
        )

        actual = list()
        async for execution in reader:
            actual.append(execution)

        self.assertEqual(0, len(actual))

    async def test_aiter_dropped_all(self):
        e0 = _me(_id=0, price=Decimal('80'), timestamp_forward=np.timedelta64(59, 's'))
        e1 = _me(_id=1, price=Decimal('110'), timestamp_forward=np.timedelta64(60, 's'))

        class _Iterator(AsyncIterable[Execution]):
            async def __aiter__(self) -> AsyncIterator[Execution]:
                yield e0
                yield e1

        reader = DropWhileStream(
            logger=get_logger(self.test_aiter_dropped_all.__name__),
            upstream=_Iterator(),
            predicate=lambda e: True
        )

        actual = list()
        async for execution in reader:
            actual.append(execution)

        self.assertEqual(0, len(actual))

    async def test_aiter(self):
        base_timestamp = np.datetime64(datetime(2000, 1, 1, 0, 0, 0))
        e0 = _me(_id=0, base_timestamp=base_timestamp, timestamp_forward=np.timedelta64(0, 's'))
        e1 = _me(_id=1, base_timestamp=base_timestamp, timestamp_forward=np.timedelta64(1, 's'))
        e2 = _me(_id=2, base_timestamp=base_timestamp, timestamp_forward=np.timedelta64(2, 's'))
        e3 = _me(_id=3, base_timestamp=base_timestamp, timestamp_forward=np.timedelta64(3, 's'))

        class _Iterator(AsyncIterable[Execution]):
            async def __aiter__(self) -> AsyncIterator[Execution]:
                yield e0
                yield e1
                yield e2
                yield e3

        reader = DropWhileStream(
            logger=get_logger(self.test_aiter.__name__),
            upstream=_Iterator(),
            predicate=lambda e: e.timestamp <= np.datetime64(datetime(2000, 1, 1, 0, 0, 1))
        )
        actual = list()
        async for execution in reader:
            actual.append(execution)

        self.assertEqual(2, len(actual))
        self.assertEqual(e2, actual[0])
        self.assertEqual(e3, actual[1])


class NewPricesStreamTestCase(unittest.IsolatedAsyncioTestCase):

    async def test_aiter_empty(self):
        class _Iterator(AsyncIterable[Execution]):
            def __aiter__(self):
                return

        reader = NewPricesStream(
            logger=get_logger(self.test_aiter_empty.__name__),
            time_window='1minute',
            upstream=_Iterator(),
        )

        actual = list()
        async for execution in reader:
            actual.append(execution)

        self.assertEqual(0, len(actual))

    async def test_aiter_nothing_within_window(self):
        e0 = _me(_id=0, price=Decimal('0'), timestamp_forward=np.timedelta64(59, 's'))
        e1 = _me(_id=1, price=Decimal('1'), timestamp_forward=np.timedelta64(120, 's'))
        e2 = _me(_id=2, price=Decimal('2'), timestamp_forward=np.timedelta64(240, 's'))

        reader = NewPricesStream(
            logger=get_logger(self.test_aiter_nothing_within_window.__name__),
            time_window='1minute',
            upstream=build_iterator([e0, e1, e2]),
        )

        actual = list()
        async for execution in reader:
            actual.append(execution)

        self.assertEqual(3, len(actual))

        # 1st time unit
        self.assertEqual(e0, actual[0])

        # 2nd time unit (nothing returned)

        # 3rd time unit
        self.assertEqual(e1, actual[1])

        # 4th time unit
        self.assertEqual(e2, actual[2])

    async def test_aiter_1execution_within_window(self):
        e0 = _me(_id=0, price=Decimal('0'), timestamp_forward=np.timedelta64(59, 's'))
        e1 = _me(_id=1, price=Decimal('1'), timestamp_forward=np.timedelta64(120, 's'))
        e2 = _me(_id=2, price=Decimal('2'), timestamp_forward=np.timedelta64(240, 's'))

        reader = NewPricesStream(
            logger=get_logger(self.test_aiter_1execution_within_window.__name__),
            time_window='1minute',
            upstream=build_iterator([e0, e1, e2]),
        )

        actual = list()
        async for execution in reader:
            actual.append(execution)

        self.assertEqual(3, len(actual))
        self.assertEqual(e0, actual[0])
        self.assertEqual(e1, actual[1])
        self.assertEqual(e2, actual[2])

    async def test_aiter_2executions_within_window(self):
        e0 = _me(_id=0, price=Decimal('1'), timestamp_forward=np.timedelta64(58, 's'))
        e1 = _me(_id=1, price=Decimal('2'), timestamp_forward=np.timedelta64(59, 's'))
        e2 = _me(_id=2, price=Decimal('3'), timestamp_forward=np.timedelta64(60, 's'))

        reader = NewPricesStream(
            logger=get_logger(self.test_aiter_2executions_within_window.__name__),
            time_window='1minute',
            upstream=build_iterator([e0, e1, e2]),
        )

        actual = list()
        async for execution in reader:
            actual.append(execution)

        self.assertEqual(3, len(actual))
        self.assertEqual(e0, actual[0])
        self.assertEqual(e1, actual[1])
        self.assertEqual(e2, actual[2])

    async def test_aiter_2executions_within_window_same_price(self):
        e0 = _me(_id=0, price=Decimal('1'), timestamp_forward=np.timedelta64(58, 's'))
        e1 = _me(_id=1, price=Decimal('1'), timestamp_forward=np.timedelta64(59, 's'))
        e2 = _me(_id=2, price=Decimal('1'), timestamp_forward=np.timedelta64(60, 's'))

        reader = NewPricesStream(
            logger=get_logger(self.test_aiter_2executions_within_window_same_price.__name__),
            time_window='1minute',
            upstream=build_iterator([e0, e1, e2]),
        )

        actual = list()
        async for execution in reader:
            actual.append(execution)

        self.assertEqual(2, len(actual))
        self.assertEqual(e0, actual[0])
        self.assertEqual(e2, actual[1])

    async def test_aiter_last_window(self):
        e0 = _me(_id=0, price=Decimal('1'), timestamp_forward=np.timedelta64(59, 's'))
        e1 = _me(_id=1, price=Decimal('2'), timestamp_forward=np.timedelta64(60, 's'))
        e2 = _me(_id=2, price=Decimal('2'), timestamp_forward=np.timedelta64(61, 's'))
        e3 = _me(_id=3, price=Decimal('3'), timestamp_forward=np.timedelta64(62, 's'))
        e4 = _me(_id=4, price=Decimal('2'), timestamp_forward=np.timedelta64(63, 's'))

        reader = NewPricesStream(
            logger=get_logger(self.test_aiter_last_window.__name__),
            time_window='1minute',
            upstream=build_iterator([e0, e1, e2, e3, e4]),
        )

        actual = list()
        async for execution in reader:
            actual.append(execution)

        self.assertEqual(3, len(actual))
        self.assertEqual(e0, actual[0])
        self.assertEqual(e1, actual[1])
        self.assertEqual(e3, actual[2])

    async def test_aiter(self):
        e0 = _me(_id=0, price=Decimal('80'), timestamp_forward=np.timedelta64(59, 's'))
        e1 = _me(_id=1, price=Decimal('110'), timestamp_forward=np.timedelta64(60, 's'))
        e2 = _me(_id=2, price=Decimal('100'), timestamp_forward=np.timedelta64(60, 's'))
        e3 = _me(_id=3, price=Decimal('200'), timestamp_forward=np.timedelta64(61, 's'))
        e4 = _me(_id=4, price=Decimal('200'), timestamp_forward=np.timedelta64(62, 's'))
        e5 = _me(_id=5, price=Decimal('100'), timestamp_forward=np.timedelta64(62, 's'))
        e6 = _me(_id=6, price=Decimal('90'), timestamp_forward=np.timedelta64(62, 's'))
        e7 = _me(_id=7, price=Decimal('100'), timestamp_forward=np.timedelta64(119, 's'))
        e8 = _me(_id=8, price=Decimal('210'), timestamp_forward=np.timedelta64(120, 's'))

        reader = NewPricesStream(
            logger=get_logger(self.test_aiter.__name__),
            time_window='1minute',
            upstream=build_iterator([e0, e1, e2, e3, e4, e5, e6, e7, e8]),
        )

        actual = list()
        async for execution in reader:
            actual.append(execution)

        self.assertEqual(6, len(actual))
        self.assertEqual(e0, actual[0])
        self.assertEqual(e1, actual[1])
        self.assertEqual(e2, actual[2])
        self.assertEqual(e3, actual[3])
        self.assertEqual(e6, actual[4])
        self.assertEqual(e8, actual[5])


class OHLCStreamTestCase(unittest.IsolatedAsyncioTestCase):

    async def test_aiter_empty(self):
        class _Iterator(AsyncIterable[Execution]):
            def __aiter__(self):
                return

        reader = OHLCStream(
            logger=get_logger(self.test_aiter_empty.__name__),
            time_window='1minute',
            upstream=_Iterator()
        )

        actual = list()
        async for execution in reader:
            actual.append(execution)

        self.assertEqual(0, len(actual))

    async def test_aiter_nothing_within_window(self):
        e0 = _me(_id=0, price=Decimal('1'), timestamp_forward=np.timedelta64(59, 's'))
        e1 = _me(_id=1, price=Decimal('2'), timestamp_forward=np.timedelta64(120, 's'))
        e2 = _me(_id=2, price=Decimal('3'), timestamp_forward=np.timedelta64(240, 's'))

        reader = OHLCStream(
            logger=get_logger(self.test_aiter_nothing_within_window.__name__),
            time_window='1minute',
            upstream=build_iterator([e0, e1, e2])
        )

        actual = list()
        async for execution in reader:
            actual.append(execution)

        self.assertEqual(8, len(actual))
        self.assertEqual(e0, actual[0])
        self.assertEqual(e0, actual[1])
        self.assertEqual(e0, actual[2])
        self.assertEqual(e0, actual[3])
        self.assertEqual(e1, actual[4])
        self.assertEqual(e1, actual[5])
        self.assertEqual(e1, actual[6])
        self.assertEqual(e1, actual[7])

    async def test_aiter_1execution_within_window(self):
        e0 = _me(_id=0, price=Decimal('80'), timestamp_forward=np.timedelta64(59, 's'))
        e1 = _me(_id=0, price=Decimal('80'), timestamp_forward=np.timedelta64(60, 's'))

        reader = OHLCStream(
            logger=get_logger(self.test_aiter_1execution_within_window.__name__),
            time_window='1minute',
            upstream=build_iterator([e0, e1])
        )

        actual = list()
        async for execution in reader:
            actual.append(execution)

        self.assertEqual(4, len(actual))
        self.assertEqual(e0, actual[0])
        self.assertEqual(e0, actual[1])
        self.assertEqual(e0, actual[2])
        self.assertEqual(e0, actual[3])

    async def test_aiter_last_window(self):
        e0 = _me(_id=0, price=Decimal('80'), timestamp_forward=np.timedelta64(59, 's'))

        reader = OHLCStream(
            logger=get_logger(self.test_aiter_last_window.__name__),
            time_window='1minute',
            upstream=build_iterator([e0])
        )

        actual = list()
        async for execution in reader:
            actual.append(execution)

        self.assertEqual(0, len(actual))

    async def test_aiter(self):
        e0 = _me(_id=0, price=Decimal('90'), timestamp_forward=np.timedelta64(58, 's'))
        e1 = _me(_id=1, price=Decimal('80'), timestamp_forward=np.timedelta64(59, 's'))
        e2 = _me(_id=2, price=Decimal('110'), timestamp_forward=np.timedelta64(60, 's'))
        e3 = _me(_id=3, price=Decimal('100'), timestamp_forward=np.timedelta64(60, 's'))
        e4 = _me(_id=4, price=Decimal('200'), timestamp_forward=np.timedelta64(61, 's'))
        e5 = _me(_id=5, price=Decimal('199'), timestamp_forward=np.timedelta64(62, 's'))
        e6 = _me(_id=6, price=Decimal('101'), timestamp_forward=np.timedelta64(62, 's'))
        e7 = _me(_id=7, price=Decimal('100'), timestamp_forward=np.timedelta64(119, 's'))
        e8 = _me(_id=8, price=Decimal('210'), timestamp_forward=np.timedelta64(120, 's'))

        reader = OHLCStream(
            logger=get_logger(self.test_aiter.__name__),
            time_window='1minute',
            upstream=build_iterator([e0, e1, e2, e3, e4, e5, e6, e7, e8])
        )

        actual = list()
        async for execution in reader:
            actual.append(execution)

        self.assertEqual(8, len(actual))
        self.assertEqual(e0, actual[0])
        self.assertEqual(e0, actual[1])
        self.assertEqual(e1, actual[2])
        self.assertEqual(e1, actual[3])
        self.assertEqual(e2, actual[4])
        self.assertEqual(e3, actual[5])
        self.assertEqual(e4, actual[6])
        self.assertEqual(e7, actual[7])


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(DropWhileStreamTestCase))
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(NewPricesStreamTestCase))
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(OHLCStreamTestCase))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='test_suite', verbosity=2, buffer=True)
