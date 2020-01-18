import asyncio
from collections import deque
from functools import partial
from logging import Logger
from typing import Deque, Iterable, Dict

import numpy as np
import pandas as pd

from trade.execution.model import Execution


class TimeWindowExecutionQueue:
    """
    保持期間付きの、約定を要素とするキュー

    `spawn_queue`を呼び出すと、クライアント専用に保持期間分の要素が確保されます。
    保持期間分およびリアルタイムの要素取得は`get`メソッドで行います。
    要素の追加は`put_nowait`メソッドで行います。追加された要素は、すべてのクライアントが取得できます。
    クライアントがこれ以上要素の取得を行わないことを`dispose_queue`メソッドの呼び出しで伝えてください。

    要素数の上限はありません。

    要素の追加中であっても、要素の取得がブロッキングされません。
    要素の取得中であっても、要素の追加がブロッキングされません。

    保持期間内の要素をすべて取得した後は、次の取得で`SwitchedToRealtime`オブジェクトが返されます。
    その後は、要素が追加されるまで取得は待機されます。

    SwitchedToRealtimeオブジェクトを返す前までは、取得する要素は時系列順であることが保証されます。
    その後の要素取得は、追加された順です（FIFO）。

    保持期間内において、古くなった要素は自動的にキューから削除されます。
    削除は、`put_nowait`が呼び出された時に行われます。
    要素が古いことは、次の方法で判定されます。「保持期間 < (追加しようとする約定timestamp - 左端要素の約定timestamp)」

    # time windows が 3 のとき、
    t0, t1, t2, t3
    t1, t2, t3, t4        <- put_nowait t4
    """

    """
    - upstream is WebSocket server like BitFlyer web socket server
    - self._deque is collections.Deque - timed window
    - self._queues is asyncio.Queue for clients

    # Warming up...

    upstream        : e1
    self._deque     :

    upstream        : e1
    self._deque     :  e1

    upstream        : e1  e2
    self._deque     :  e1

    upstream        : e1  e2
    self._deque     :  e1  e2

    upstream        : e1  e2
    self._deque     :      e2             <- rotated

    upstream        : e1  e2  e3
    self._deque     :      e2

    upstream        : e1  e2  e3
    self._deque     :      e2  e3

    <-- Connect from subscriber-A

    upstream        : e1  e2  e3  e4
    self._deque     :      e2  e3
    self._queues[A] :

    upstream        : e1  e2  e3  e4
    self._deque     :      e2  e3  e4
    self._queues[A] :       e2  e3

    --> Popped `e2`,`e3` by subscriber-A

    upstream        : e1  e2  e3  e4
    self._deque     :      e2  e3  e4
    self._queues[A] :               e4

    --> Popped `e4` by subscriber-A

    upstream        : e1  e2  e3  e4
    self._deque     :      e2  e3  e4
    self._queues[A] :

    --> Append `SW`

    upstream        : e1  e2  e3  e4
    self._deque     :      e2  e3  e4
    self._queues[A] :               SW

    --> Popped `SW` by subscriber-A

    upstream        : e1  e2  e3  e4
    self._deque     :      e2  e3  e4
    self._queues[A] :

    upstream        : e1  e2  e3  e4  e5
    self._deque     :      e2  e3  e4  e5
    self._queues[A] :                   e5

    <-- Close from subscriber-A

    upstream        : e1  e2  e3  e4  e5
    self._deque     :      e2  e3  e4  e5
    """

    def __init__(self, logger: Logger,
                 time_window: str,
                 switched_to_realtime_partial: partial,
                 loop=None):
        self._logger = logger
        self._time_window = pd.to_timedelta(time_window).to_timedelta64()
        self._switched_to_realtime_partial = switched_to_realtime_partial
        self._loop = loop
        self._window_satisfied = False

        # Queue for holding execution within the time window
        self._deque: Deque[Execution] = deque()

        # Queue for each clients
        self._queues: Dict[str, _Queue] = dict()

        self._switched_to_realtime: Dict[str, bool] = dict()

    def spawn_queue(self, client_id: str):
        self._queues[client_id] = _Queue(loop=self._loop)
        self._queues[client_id].init(self._deque)
        self._switched_to_realtime[client_id] = False

    def dispose_queue(self, client_id: str):
        del self._queues[client_id]

    def put_nowait(self, execution: Execution):
        if not self._deque:
            self._deque.append(execution)

        else:
            # Inserting execution
            idx = len(self._deque)
            while 0 < idx:
                left = self._deque[idx - 1]

                if left.timestamp < execution.timestamp:
                    self._deque.insert(idx, execution)
                    break

                if left.timestamp == execution.timestamp:
                    self._deque.append(execution)
                    break

                idx -= 1
            else:
                self._deque.appendleft(execution)

            # Dispose old executions
            n_pops = 0
            most_right = self._deque[-1]
            for n, deque_execution in enumerate(self._deque):
                delta = most_right.timestamp - deque_execution.timestamp
                if delta <= self._time_window:
                    break
                n_pops += 1
            [self._deque.popleft() for _ in range(n_pops)]

            if not self._window_satisfied:
                if n_pops:
                    self._window_satisfied = True
                    self._logger.info('time window satisfied')

        # Put to all client queues
        [q.put_nowait(execution) for q in self._queues.values()]

    async def get(self, client_id: str):
        # Put SW
        if not self._switched_to_realtime[client_id]:
            if self._queues[client_id].qsize() == 0:
                self._queues[client_id].put_nowait(
                    self._switched_to_realtime_partial(timestamp=np.datetime64('now', 'ns', utc=True))
                )
                self._switched_to_realtime[client_id] = True

        return await self._queues[client_id].get()

    def spawned_queue_count(self):
        return len(self._queues)

    def execution_count(self):
        return len(self._deque)


class _Queue(asyncio.Queue):

    def init(self, iterable: Iterable):
        self._queue = deque(iterable)
