import unittest
from typing import AsyncIterable, AsyncIterator

import numpy as np

from trade.execution.model import Execution
from trade.execution.stream.adapter.sync import SynchronizedStream
from trade.log import get_logger
from trade.model import Symbol
from trade.test_helper import make_execution, make_execution_s


class SynchronizedStreamTestCase(unittest.IsolatedAsyncioTestCase):

    def _build_reader(self, method_name, primary_iterable, secondary_iterable):
        return SynchronizedStream(
            logger=get_logger(method_name),
            primary_iterable=primary_iterable,
            secondary_iterable=secondary_iterable,
        )

    async def test_aiter(self):
        """
        iter-p : p0(t0)  p2(t1)  p4(t2)  SI
        iter-s : s1(t1)  s3(t1)  s5(t1)  s6(t2)  s7(t3)

        output : (p0(t0), N)  (p2(t1), s5(t1))  (p4(t2), s6(t2))
        """
        p0_t0 = make_execution(symbol=Symbol.FXBTCJPY, _id=0)
        p2_t1 = make_execution(symbol=Symbol.FXBTCJPY, _id=2, timestamp_forward=np.timedelta64(1, 'ns'))
        p4_t2 = make_execution(symbol=Symbol.FXBTCJPY, _id=4, timestamp_forward=np.timedelta64(2, 'ns'))
        s1_t1 = make_execution_s(symbol=Symbol.BTCJPY, _id=1, timestamp_forward=np.timedelta64(1, 'ns'))
        s3_t1 = make_execution_s(symbol=Symbol.BTCJPY, _id=3, timestamp_forward=np.timedelta64(1, 'ns'))
        s5_t1 = make_execution_s(symbol=Symbol.BTCJPY, _id=5, timestamp_forward=np.timedelta64(1, 'ns'))
        s6_t2 = make_execution_s(symbol=Symbol.BTCJPY, _id=6, timestamp_forward=np.timedelta64(2, 'ns'))
        s7_t3 = make_execution_s(symbol=Symbol.BTCJPY, _id=7, timestamp_forward=np.timedelta64(3, 'ns'))

        class IteratorPrimary(AsyncIterable[Execution]):
            async def __aiter__(self) -> AsyncIterator[Execution]:
                yield p0_t0
                yield p2_t1
                yield p4_t2

        class IteratorSecondary(AsyncIterable[Execution]):
            async def __aiter__(self) -> AsyncIterator[Execution]:
                yield s1_t1
                yield s3_t1
                yield s5_t1
                yield s6_t2
                yield s7_t3

        reader = self._build_reader(self.test_aiter.__name__, IteratorPrimary(), IteratorSecondary())
        actual = []
        async for execution in reader:
            actual.append(execution)

        self.assertEqual(3, len(actual))
        self.assertEqual(Execution.wrap(p0_t0, synchronized_execution=None), actual[0])
        self.assertEqual(Execution.wrap(p2_t1, synchronized_execution=s5_t1), actual[1])
        self.assertEqual(Execution.wrap(p4_t2, synchronized_execution=s6_t2), actual[2])

    async def test_aiter_primary_same_timestamps(self):
        """
        iter-p : p0(t0)  p2(t1)  p4(t1)  SI
        iter-s : s1(t1)  s3(t1)  s5(t1)  s6(t2)  s7(t3)

        output : (p0(t0), N)  (p2(t1), s5(t1))  (p4(t1), s5(t1))
        """
        p0_t0 = make_execution(symbol=Symbol.FXBTCJPY, _id=0)
        p2_t1 = make_execution(symbol=Symbol.FXBTCJPY, _id=2, timestamp_forward=np.timedelta64(1, 'ns'))
        p4_t1 = make_execution(symbol=Symbol.FXBTCJPY, _id=4, timestamp_forward=np.timedelta64(1, 'ns'))
        s1_t1 = make_execution_s(symbol=Symbol.BTCJPY, _id=1, timestamp_forward=np.timedelta64(1, 'ns'))
        s3_t1 = make_execution_s(symbol=Symbol.BTCJPY, _id=3, timestamp_forward=np.timedelta64(1, 'ns'))
        s5_t1 = make_execution_s(symbol=Symbol.BTCJPY, _id=5, timestamp_forward=np.timedelta64(1, 'ns'))
        s6_t2 = make_execution_s(symbol=Symbol.BTCJPY, _id=6, timestamp_forward=np.timedelta64(2, 'ns'))
        s7_t3 = make_execution_s(symbol=Symbol.BTCJPY, _id=7, timestamp_forward=np.timedelta64(3, 'ns'))

        class IteratorPrimary(AsyncIterable[Execution]):
            async def __aiter__(self) -> AsyncIterator[Execution]:
                yield p0_t0
                yield p2_t1
                yield p4_t1

        class IteratorSecondary(AsyncIterable[Execution]):
            async def __aiter__(self) -> AsyncIterator[Execution]:
                yield s1_t1
                yield s3_t1
                yield s5_t1
                yield s6_t2
                yield s7_t3

        reader = self._build_reader(
            self.test_aiter_primary_same_timestamps.__name__, IteratorPrimary(), IteratorSecondary()
        )
        actual = []
        async for execution in reader:
            actual.append(execution)

        self.assertEqual(3, len(actual))
        self.assertEqual(Execution.wrap(p0_t0, synchronized_execution=None), actual[0])
        self.assertEqual(Execution.wrap(p2_t1, synchronized_execution=s5_t1), actual[1])
        self.assertEqual(Execution.wrap(p4_t1, synchronized_execution=s5_t1), actual[2])

    async def test_aiter_empty_primary(self):
        """
        iter-p : SI
        iter-s : s1(t1)

        output :
        """
        s1_t1 = make_execution(symbol=Symbol.BTCJPY, _id=1, timestamp_forward=np.timedelta64(1, 'ns'))

        class IteratorPrimary(AsyncIterable[Execution]):
            def __aiter__(self):
                return

        class IteratorSecondary(AsyncIterable[Execution]):
            async def __aiter__(self) -> AsyncIterator[Execution]:
                yield s1_t1

        reader = self._build_reader(self.test_aiter_empty_primary.__name__, IteratorPrimary(), IteratorSecondary())
        actual = []
        async for execution in reader:
            actual.append(execution)

        self.assertEqual(0, len(actual))

    async def test_aiter_empty_secondary(self):
        """
        iter-p : p1(t1)
        iter-s : SI

        output :
        """
        p1_t1 = make_execution(symbol=Symbol.FXBTCJPY, _id=1, timestamp_forward=np.timedelta64(1, 'ns'))

        class IteratorPrimary(AsyncIterable[Execution]):
            async def __aiter__(self) -> AsyncIterator[Execution]:
                yield p1_t1

        class IteratorSecondary(AsyncIterable[Execution]):
            def __aiter__(self):
                return

        reader = self._build_reader(self.test_aiter_empty_secondary.__name__, IteratorPrimary(), IteratorSecondary())
        actual = []
        async for execution in reader:
            actual.append(execution)

        self.assertEqual(0, len(actual))

    async def test_aiter_primary_delaying(self):
        """
        iter-p : p100(t100)  p101(t101)  SI
        iter-s : s0(t0)      s2(t1)      s3(t99)  s4(t100)  s5(t100)  s6(t101)  SI

        output : (p100(t100), s4(t100))  (p101(t101), s6(t101))
        """
        p100_t100 = make_execution(symbol=Symbol.FXBTCJPY, _id=100, timestamp_forward=np.timedelta64(100, 'ns'))
        p101_t101 = make_execution(symbol=Symbol.FXBTCJPY, _id=101, timestamp_forward=np.timedelta64(101, 'ns'))
        s0_t0 = make_execution_s(symbol=Symbol.BTCJPY, _id=0)
        s2_t1 = make_execution_s(symbol=Symbol.BTCJPY, _id=2, timestamp_forward=np.timedelta64(1, 'ns'))
        s3_t99 = make_execution_s(symbol=Symbol.BTCJPY, _id=3, timestamp_forward=np.timedelta64(99, 'ns'))
        s4_t100 = make_execution_s(symbol=Symbol.BTCJPY, _id=4, timestamp_forward=np.timedelta64(100, 'ns'))
        s5_t100 = make_execution_s(symbol=Symbol.BTCJPY, _id=5, timestamp_forward=np.timedelta64(100, 'ns'))
        s6_t101 = make_execution_s(symbol=Symbol.BTCJPY, _id=6, timestamp_forward=np.timedelta64(101, 'ns'))

        class IteratorPrimary(AsyncIterable[Execution]):
            async def __aiter__(self) -> AsyncIterator[Execution]:
                yield p100_t100
                yield p101_t101

        class IteratorSecondary(AsyncIterable[Execution]):
            async def __aiter__(self) -> AsyncIterator[Execution]:
                yield s0_t0
                yield s2_t1
                yield s3_t99
                yield s4_t100
                yield s5_t100
                yield s6_t101

        reader = self._build_reader(self.test_aiter_primary_delaying.__name__, IteratorPrimary(), IteratorSecondary())
        actual = []
        async for execution in reader:
            actual.append(execution)

        self.assertEqual(2, len(actual))
        self.assertEqual(Execution.wrap(p100_t100, synchronized_execution=s5_t100), actual[0])
        self.assertEqual(Execution.wrap(p101_t101, synchronized_execution=s6_t101), actual[1])

    async def test_aiter_secondary_delaying(self):
        """
        iter-p : p0(t0)      p2(t1)      p3(t99)  p4(t100)  p5(t100)  p6(t101)
        iter-s : s100(t100)  s101(t101)  SI

        output : (p0(t0), None)  (p2(t1), None)  (p3(t99), None)  (p4(t100), s100(t100))  (p5(t100), s100(t100))
                 (p6(t101), s101(t101))  SI
        """
        p0_t0 = make_execution(symbol=Symbol.FXBTCJPY, _id=0)
        p2_t1 = make_execution(symbol=Symbol.FXBTCJPY, _id=2, timestamp_forward=np.timedelta64(1, 'ns'))
        p3_t99 = make_execution(symbol=Symbol.FXBTCJPY, _id=3, timestamp_forward=np.timedelta64(99, 'ns'))
        p4_t100 = make_execution(symbol=Symbol.FXBTCJPY, _id=4, timestamp_forward=np.timedelta64(100, 'ns'))
        p5_t100 = make_execution(symbol=Symbol.FXBTCJPY, _id=5, timestamp_forward=np.timedelta64(100, 'ns'))
        p6_t101 = make_execution(symbol=Symbol.FXBTCJPY, _id=6, timestamp_forward=np.timedelta64(101, 'ns'))
        s100_t100 = make_execution_s(symbol=Symbol.BTCJPY, _id=100, timestamp_forward=np.timedelta64(100, 'ns'))
        s101_t101 = make_execution_s(symbol=Symbol.BTCJPY, _id=101, timestamp_forward=np.timedelta64(101, 'ns'))

        class IteratorPrimary(AsyncIterable[Execution]):
            async def __aiter__(self) -> AsyncIterator[Execution]:
                yield p0_t0
                yield p2_t1
                yield p3_t99
                yield p4_t100
                yield p5_t100
                yield p6_t101

        class IteratorSecondary(AsyncIterable[Execution]):
            async def __aiter__(self) -> AsyncIterator[Execution]:
                yield s100_t100
                yield s101_t101

        reader = self._build_reader(self.test_aiter_secondary_delaying.__name__, IteratorPrimary(), IteratorSecondary())
        actual = []
        async for execution in reader:
            actual.append(execution)

        self.assertEqual(6, len(actual))
        self.assertEqual(Execution.wrap(p0_t0, synchronized_execution=None), actual[0])
        self.assertEqual(Execution.wrap(p2_t1, synchronized_execution=None), actual[1])
        self.assertEqual(Execution.wrap(p3_t99, synchronized_execution=None), actual[2])
        self.assertEqual(Execution.wrap(p4_t100, synchronized_execution=s100_t100), actual[3])
        self.assertEqual(Execution.wrap(p5_t100, synchronized_execution=s100_t100), actual[4])
        self.assertEqual(Execution.wrap(p6_t101, synchronized_execution=s101_t101), actual[5])


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(SynchronizedStreamTestCase))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='test_suite', verbosity=2, buffer=True)
