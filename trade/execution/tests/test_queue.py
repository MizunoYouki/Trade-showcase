import asyncio
import unittest
from datetime import datetime
from decimal import Decimal
from functools import partial
from test.test_asyncio import utils as test_utils

import numpy as np

from trade.execution.model import Execution, SwitchedToRealtime
from trade.side import Side
from trade.model import Symbol
from trade.execution.queue import TimeWindowExecutionQueue
from trade.log import get_logger


class _QueueTestBase(test_utils.TestCase):

    def setUp(self):
        super().setUp()
        self.loop = self.new_test_loop()

        self._logger = get_logger(__name__)
        self._e1 = Execution(
            symbol=Symbol.FXBTCJPY, _id=1, timestamp=np.datetime64(datetime(2000, 1, 1), 'ns', utc=True),
            side=Side.BUY, price=Decimal('100'), size=Decimal('0.1'),
            buy_child_order_acceptance_id='b1', sell_child_order_acceptance_id='s1'
        )
        self._e2 = Execution(
            symbol=Symbol.FXBTCJPY, _id=2, timestamp=np.datetime64(datetime(2000, 1, 2), 'ns', utc=True),
            side=Side.BUY, price=Decimal('100'), size=Decimal('0.1'),
            buy_child_order_acceptance_id='b2', sell_child_order_acceptance_id='s2'
        )
        self._e3 = Execution(
            symbol=Symbol.FXBTCJPY, _id=3, timestamp=np.datetime64(datetime(2000, 1, 3), 'ns', utc=True),
            side=Side.BUY, price=Decimal('100'), size=Decimal('0.1'),
            buy_child_order_acceptance_id='b3', sell_child_order_acceptance_id='s3'
        )


class TimeWindowExecutionQueueTestCase(_QueueTestBase):

    def test_put_nowait(self):
        q = TimeWindowExecutionQueue(
            logger=get_logger(__name__), time_window='3days',
            switched_to_realtime_partial=partial(SwitchedToRealtime, symbol=Symbol.FXBTCJPY), loop=self.loop)
        q.put_nowait(self._e1)

        with self.assertWarns(DeprecationWarning):
            q.spawn_queue('A')

        async def queue_get():
            return await q.get('A')

        result = self.loop.run_until_complete(queue_get())
        self.assertEqual(self._e1, result)

        result = self.loop.run_until_complete(queue_get())
        self.assertTrue(isinstance(result, SwitchedToRealtime))

        q.put_nowait(self._e2)
        result = self.loop.run_until_complete(queue_get())
        self.assertEqual(self._e2, result)

    def test_order(self):
        q = TimeWindowExecutionQueue(
            logger=get_logger(__name__), time_window='3days',
            switched_to_realtime_partial=partial(SwitchedToRealtime, symbol=Symbol.FXBTCJPY), loop=self.loop)
        q.put_nowait(self._e1)
        q.put_nowait(self._e2)
        q.put_nowait(self._e3)

        with self.assertWarns(DeprecationWarning):
            q.spawn_queue('A')

        results = []

        async def queue_get():
            for _ in range(3):
                results.append(await q.get('A'))

        self.loop.run_until_complete(queue_get())
        self.assertEqual([self._e1, self._e2, self._e3], results)

    def test_order_same_timestamp(self):
        e4 = Execution(
            symbol=Symbol.FXBTCJPY, _id=4, timestamp=np.datetime64(datetime(2000, 1, 3), 'ns', utc=True),
            side=Side.BUY, price=Decimal('100'), size=Decimal('0.1'),
            buy_child_order_acceptance_id='b4', sell_child_order_acceptance_id='s4'
        )

        q = TimeWindowExecutionQueue(
            logger=get_logger(__name__), time_window='3days',
            switched_to_realtime_partial=partial(SwitchedToRealtime, symbol=Symbol.FXBTCJPY), loop=self.loop)
        q.put_nowait(self._e3)
        q.put_nowait(e4)

        with self.assertWarns(DeprecationWarning):
            q.spawn_queue('A')

        results = []

        async def queue_get():
            for _ in range(2):
                results.append(await q.get('A'))

        self.loop.run_until_complete(queue_get())
        self.assertEqual([self._e3, e4], results)

    def test_order_sorted(self):
        q = TimeWindowExecutionQueue(
            logger=get_logger(__name__), time_window='3days',
            switched_to_realtime_partial=partial(SwitchedToRealtime, symbol=Symbol.FXBTCJPY), loop=self.loop)
        q.put_nowait(self._e1)
        q.put_nowait(self._e3)
        q.put_nowait(self._e2)

        with self.assertWarns(DeprecationWarning):
            q.spawn_queue('A')

        results = []

        async def queue_get():
            for _ in range(3):
                results.append(await q.get('A'))

        self.loop.run_until_complete(queue_get())
        self.assertEqual([self._e1, self._e2, self._e3], results)

    def test_order_sorted2(self):
        q = TimeWindowExecutionQueue(
            logger=get_logger(__name__), time_window='3days',
            switched_to_realtime_partial=partial(SwitchedToRealtime, symbol=Symbol.FXBTCJPY), loop=self.loop)
        q.put_nowait(self._e3)
        q.put_nowait(self._e2)
        q.put_nowait(self._e1)

        with self.assertWarns(DeprecationWarning):
            q.spawn_queue('A')

        results = []

        async def queue_get():
            for _ in range(3):
                results.append(await q.get('A'))

        self.loop.run_until_complete(queue_get())
        self.assertEqual([self._e1, self._e2, self._e3], results)

    def test_get_without_connect(self):
        q = TimeWindowExecutionQueue(
            logger=get_logger(__name__), time_window='3days',
            switched_to_realtime_partial=partial(SwitchedToRealtime, symbol=Symbol.FXBTCJPY), loop=self.loop)
        q.put_nowait(self._e1)

        async def queue_get():
            return await q.get('A')

        self.assertRaises(KeyError, self.loop.run_until_complete, queue_get())

    def test_close(self):
        q = TimeWindowExecutionQueue(
            logger=get_logger(__name__), time_window='3days',
            switched_to_realtime_partial=partial(SwitchedToRealtime, symbol=Symbol.FXBTCJPY), loop=self.loop)
        q.put_nowait(self._e1)

        with self.assertWarns(DeprecationWarning):
            q.spawn_queue('A')

        async def queue_get():
            return await q.get('A')

        result = self.loop.run_until_complete(queue_get())
        self.assertEqual(self._e1, result)

        q.dispose_queue('A')
        self.assertRaises(KeyError, self.loop.run_until_complete, queue_get())

    def test_old_executions_are_disposed(self):
        q = TimeWindowExecutionQueue(
            logger=get_logger(__name__), time_window='1days',
            switched_to_realtime_partial=partial(SwitchedToRealtime, symbol=Symbol.FXBTCJPY), loop=self.loop)
        q.put_nowait(self._e1)
        q.put_nowait(self._e2)
        q.put_nowait(self._e3)

        with self.assertWarns(DeprecationWarning):
            q.spawn_queue('A')

        results = []

        async def queue_get():
            for _ in range(3):
                results.append(await q.get('A'))

        self.loop.run_until_complete(queue_get())
        self.assertEqual(3, len(results))
        self.assertEqual(self._e2, results[0])
        self.assertEqual(self._e3, results[1])
        self.assertTrue(isinstance(results[2], SwitchedToRealtime))

    def test_blocking_get(self):
        def gen():
            yield 0.1
            yield 0.1
            yield 0.1
            yield 0.1

        self.loop = self.new_test_loop(gen)

        q = TimeWindowExecutionQueue(
            logger=get_logger(__name__), time_window='3days',
            switched_to_realtime_partial=partial(SwitchedToRealtime, symbol=Symbol.FXBTCJPY), loop=self.loop)
        q.put_nowait(self._e1)

        with self.assertWarns(DeprecationWarning):
            q.spawn_queue('A')

        results = []

        async def queue_get():
            results.append(await asyncio.wait_for(q.get('A'), 0.1))
            results.append(await asyncio.wait_for(q.get('A'), 0.1))
            with self.assertRaises(asyncio.TimeoutError):
                await asyncio.wait_for(q.get('A'), 0.1)

        self.loop.run_until_complete(queue_get())
        self.assertEqual(2, len(results))
        self.assertEqual(self._e1, results[0])
        self.assertTrue(isinstance(results[1], SwitchedToRealtime))

    def test_nonblocking_get_while_putting(self):
        def gen():
            yield 0.1
            yield 0.1

        self.loop = self.new_test_loop(gen)
        q = TimeWindowExecutionQueue(
            logger=get_logger(__name__), time_window='3days',
            switched_to_realtime_partial=partial(SwitchedToRealtime, symbol=Symbol.FXBTCJPY), loop=self.loop)
        q.put_nowait(self._e1)

        with self.assertWarns(DeprecationWarning):
            q.spawn_queue('A')

        results = []

        async def putter():
            await asyncio.sleep(0.1)
            q.put_nowait(self._e2)

        async def getter():
            results.append(await q.get('A'))
            results.append(await q.get('A'))
            results.append(await q.get('A'))

        async def test():
            await asyncio.gather(putter(), getter(), loop=self.loop)

        self.loop.run_until_complete(test())
        self.assertEqual(3, len(results))
        self.assertEqual(self._e1, results[0])
        self.assertTrue(isinstance(results[1], SwitchedToRealtime))
        self.assertEqual(self._e2, results[2])

    def test_nonblocking_put_while_getting(self):
        def gen():
            yield 0.1
            yield 0.1

        self.loop = self.new_test_loop(gen)
        q = TimeWindowExecutionQueue(
            logger=get_logger(__name__), time_window='3days',
            switched_to_realtime_partial=partial(SwitchedToRealtime, symbol=Symbol.FXBTCJPY), loop=self.loop)
        q.put_nowait(self._e1)

        with self.assertWarns(DeprecationWarning):
            q.spawn_queue('A')

        results = []

        async def putter():
            q.put_nowait(self._e2)

        async def getter():
            await asyncio.sleep(0.1)
            results.append(await q.get('A'))
            results.append(await q.get('A'))
            results.append(await q.get('A'))

        async def test():
            await asyncio.gather(getter(), putter(), loop=self.loop)

        self.loop.run_until_complete(test())
        self.assertEqual(3, len(results))
        self.assertEqual(self._e1, results[0])
        self.assertEqual(self._e2, results[1])
        self.assertTrue(isinstance(results[2], SwitchedToRealtime))

    def test_dispose_nonexisting_queue(self):
        q = TimeWindowExecutionQueue(
            logger=get_logger(__name__), time_window='3days',
            switched_to_realtime_partial=partial(SwitchedToRealtime, symbol=Symbol.FXBTCJPY), loop=self.loop)

        self.assertRaises(KeyError, q.dispose_queue, 'A')

    def test_spawned_queue_count(self):
        q = TimeWindowExecutionQueue(
            logger=get_logger(__name__), time_window='3days',
            switched_to_realtime_partial=partial(SwitchedToRealtime, symbol=Symbol.FXBTCJPY), loop=self.loop)

        self.assertEqual(0, q.spawned_queue_count())

        with self.assertWarns(DeprecationWarning):
            q.spawn_queue('A')
        self.assertEqual(1, q.spawned_queue_count())

        q.dispose_queue('A')
        self.assertEqual(0, q.spawned_queue_count())

        with self.assertWarns(DeprecationWarning):
            q.spawn_queue('A')
        self.assertEqual(1, q.spawned_queue_count())

        with self.assertWarns(DeprecationWarning):
            q.spawn_queue('B')
        self.assertEqual(2, q.spawned_queue_count())

        q.dispose_queue('A')
        self.assertEqual(1, q.spawned_queue_count())

    def test_blocking_get_until_time_window_satisfied(self):
        # TODO:
        pass


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TimeWindowExecutionQueueTestCase))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='test_suite', verbosity=2, buffer=True)
