import asyncio
import unittest
from typing import AsyncIterable, AsyncIterator

import numpy as np

from trade.execution.model import Execution
from trade.execution.stream.chain import ChainedStream
from trade.log import get_logger
from trade.model import Symbol
from trade.test_helper import make_execution


class ChainedStreamTestCase(unittest.IsolatedAsyncioTestCase):

    async def test_aiter_timestamp_order_is_not_ascend(self):
        e0 = make_execution(symbol=Symbol.FXBTCJPY, _id=0)
        e1 = make_execution(symbol=Symbol.FXBTCJPY, _id=1)
        e2 = make_execution(symbol=Symbol.FXBTCJPY, _id=2, timestamp_forward=np.timedelta64(1, 'ns'))

        class Iterator1(AsyncIterable[Execution]):
            async def __aiter__(self) -> AsyncIterator[Execution]:
                yield e0
                yield e1
                yield e2

        class Iterator2(AsyncIterable[Execution]):

            async def __aiter__(self) -> AsyncIterator[Execution]:
                yield make_execution(symbol=Symbol.FXBTCJPY, _id=3)
                yield make_execution(symbol=Symbol.FXBTCJPY, _id=4, timestamp_forward=np.timedelta64(1, 'ns'))

        reader = ChainedStream(
            logger=get_logger(self.test_aiter_timestamp_order_is_not_ascend.__name__),
            upstreams=[Iterator1(), Iterator2()]
        )

        actual = list()

        async def read():
            async for execution in reader:
                actual.append(execution)

        with self.assertRaises(ValueError):
            await asyncio.gather(
                asyncio.create_task(read())
            )

        self.assertEqual(3, len(actual))
        self.assertEqual(e0, actual[0])
        self.assertEqual(e1, actual[1])
        self.assertEqual(e2, actual[2])

    async def test_aiter(self):
        e0 = make_execution(symbol=Symbol.FXBTCJPY, _id=0)
        e1 = make_execution(symbol=Symbol.FXBTCJPY, _id=1)
        e2 = make_execution(symbol=Symbol.FXBTCJPY, _id=2, timestamp_forward=np.timedelta64(1, 'ns'))
        e3 = make_execution(symbol=Symbol.FXBTCJPY, _id=3, timestamp_forward=np.timedelta64(1, 'ns'))
        e4 = make_execution(symbol=Symbol.FXBTCJPY, _id=4, timestamp_forward=np.timedelta64(2, 'ns'))

        class Iterator1(AsyncIterable[Execution]):
            async def __aiter__(self) -> AsyncIterator[Execution]:
                yield e0
                yield e1
                yield e2

        class Iterator2(AsyncIterable[Execution]):

            async def __aiter__(self) -> AsyncIterator[Execution]:
                yield e3
                yield e4

        reader = ChainedStream(
            logger=get_logger(self.test_aiter.__name__),
            upstreams=[Iterator1(), Iterator2()]
        )

        actual = list()

        async for execution in reader:
            actual.append(execution)

        self.assertEqual(5, len(actual))
        self.assertEqual(e0, actual[0])
        self.assertEqual(e1, actual[1])
        self.assertEqual(e2, actual[2])
        self.assertEqual(e3, actual[3])
        self.assertEqual(e4, actual[4])


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(ChainedStreamTestCase))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='test_suite', verbosity=2, buffer=True)
