import os
from datetime import datetime
from decimal import Decimal
from typing import Optional, List, AsyncIterable, AsyncIterator
from unittest import TestCase

import numpy as np

from trade.execution.model import Execution
from trade.execution.model import SynchronizedExecution
from trade.model import Symbol
from trade.side import Side


class Path:

    def __init__(self, base_path):
        self._base_path = os.path.abspath(base_path)

    def abs(self, path) -> str:
        return os.path.join(self._base_path, path)


def make_execution(symbol: Symbol,
                   _id: int,
                   timestamp_forward: np.timedelta64 = np.timedelta64(0, 'ns'),
                   price: Decimal = Decimal('100.0'),
                   sync_execution: Optional[SynchronizedExecution] = None,
                   base_timestamp: Optional[np.datetime64] = None) -> Execution:
    if not base_timestamp:
        base_timestamp = np.datetime64(datetime(2000, 1, 1, 0, 0, 0), 'ns', utc=True)

    return Execution(
        symbol=symbol,
        _id=_id,
        timestamp=base_timestamp + timestamp_forward,
        side=Side.BUY,
        price=price,
        size=Decimal('0.1'),
        buy_child_order_acceptance_id=f'B{_id}',
        sell_child_order_acceptance_id=f'S{_id}',
        synchronized_execution_time_delta=(sync_execution
                                           and sync_execution.timestamp - (base_timestamp + timestamp_forward) or None),
        synchronized_execution_price_deviation=(sync_execution
                                                and ((price - sync_execution.price) / sync_execution.price) or None),
        synchronized_execution=sync_execution and sync_execution or None
    )


def make_execution_s(symbol,
                     _id,
                     timestamp_forward: np.timedelta64 = np.timedelta64(0, 'ns'),
                     price: Decimal = Decimal('100.0'),
                     base_timestamp: Optional[np.datetime64] = None) -> SynchronizedExecution:
    e: Execution = make_execution(
        symbol=symbol, _id=_id, timestamp_forward=timestamp_forward, price=price, base_timestamp=base_timestamp
    )

    return SynchronizedExecution(
        symbol=e.symbol,
        _id=e._id,
        timestamp=e.timestamp,
        side=e.side,
        price=e.price,
        size=e.size,
        buy_child_order_acceptance_id=e.buy_child_order_acceptance_id,
        sell_child_order_acceptance_id=e.sell_child_order_acceptance_id
    )


def assert_signal(test_case: TestCase, actual, **expected):
    for attr, e in expected.items():
        test_case.assertEqual(e, getattr(actual, attr), f'attr is {attr}')


def build_iterator(executions: List[Execution]) -> AsyncIterable[Execution]:
    class _Iterator(AsyncIterable[Execution]):
        async def __aiter__(self) -> AsyncIterator[Execution]:
            while executions:
                e = executions.pop(0)
                yield e

    return _Iterator()
