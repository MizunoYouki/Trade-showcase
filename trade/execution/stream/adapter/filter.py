from logging import Logger
from typing import AsyncIterable, AsyncIterator, Optional, List, Callable

import pandas as pd

from trade.execution.model import Execution


class DropWhileStream(AsyncIterable[Execution]):
    """
    predicate (述語) がTrueである間は要素を飛ばし、一度でもFalseとなった以降は全ての要素を返す、Executionストリームアダプター
    """

    def __init__(self, logger: Logger,
                 upstream: AsyncIterable[Execution],
                 predicate: Callable[[Execution], bool]):
        self._logger = logger
        self._upstream = upstream
        self._predicate = predicate

    async def __aiter__(self) -> AsyncIterator[Execution]:
        if not self._upstream.__aiter__():
            return

        done = False

        async for execution in self._upstream:
            if not done:
                if not self._predicate(execution):
                    done = True
                    yield execution
            else:
                yield execution


class NewPricesStream(AsyncIterable[Execution]):
    """
    タイムウインドウ内の新高値および新安値 を返すExecutionストリームアダプター

    タイムウインドウ内にExecutionが存在しない場合、何も返されません。
    タイムウインドウ内にExecutionが1つだけ存在する場合、1つ返されます。
    タイムウインドウ内にExecutionが2つだけ存在する場合、2つ返されます。
    タイムウインドウ内にExecutionが3つ以上存在する場合、新高値および新安値を更新する度に返されます。
    """

    def __init__(self, logger: Logger,
                 upstream: AsyncIterable[Execution],
                 time_window: str):
        self._logger = logger
        self._upstream = upstream
        self._time_window = pd.to_timedelta(time_window)

    async def __aiter__(self) -> AsyncIterator[Execution]:
        if not self._upstream.__aiter__():
            return

        prev: Optional[Execution] = None
        prev_units: Optional[int] = None
        high: Optional[Execution] = None
        low: Optional[Execution] = None

        async for execution in self._upstream:
            units = execution.timestamp.item() // self._time_window.value

            if not prev or prev_units != units:
                high = execution
                low = execution
                yield execution

            elif high.price < execution.price:
                high = execution
                yield high

            elif execution.price < low.price:
                low = execution
                yield low

            prev = execution
            prev_units = units


class OHLCStream(AsyncIterable[Execution]):
    """
    タイムウインドウ内のOHLC4要素だけを返す、Executionストリームアダプター

    タイムウインドウ内にExecutionが存在しない場合、何も返されません。

    タイムウインドウ内にExecutionが1つ以上存在する場合、必ず4つの要素が返されます。

    最初に返されるのは、ローテーションが起こる前の未完全なOHLCバーの4要素です。

    最後に返されるのは、ローテーションが済んだ完全なOHLCバーの値です。
    末尾のOHLCバー要素は返されません。
    """

    def __init__(self, logger: Logger, upstream: AsyncIterable[Execution], time_window: str):
        self._logger = logger
        self._upstream = upstream
        self._time_window = pd.to_timedelta(time_window)

    async def __aiter__(self) -> AsyncIterator[Execution]:
        executions: List[Execution] = list()
        prev_units: Optional[int] = None

        if not self._upstream.__aiter__():
            return

        async for execution in self._upstream:
            units = execution.timestamp.item() // self._time_window.value

            if prev_units is None:
                executions.append(execution)
                prev_units = units
                continue

            if prev_units != units:
                _open = executions[0]
                high = max(executions, key=lambda e: e.price)
                low = min(executions, key=lambda e: e.price)
                close = executions[-1]

                yield _open

                if high.timestamp <= low.timestamp:
                    yield high
                    yield low
                else:
                    yield low
                    yield high

                yield close

                executions.clear()

            executions.append(execution)
            prev_units = units
