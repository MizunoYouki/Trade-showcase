from logging import Logger
from typing import AsyncIterable, Sequence, AsyncIterator, Optional

from trade.execution.model import Execution


class ChainedStream(AsyncIterable[Execution]):
    """
    連結された、Executionストリーム

    先頭のupstreamの全Executionオブジェクトを返し、次に2番目のupstreamの全Executionオブジェクトを返し、と全upstreamsの
    全Executionオブジェクトを返します。
    upstreamがイテレーションされた時に、Executionオブジェクトのタイムスタンプが昇順でない場合、ValueError例外が送出されます。
    """

    def __init__(self, logger: Logger, upstreams: Sequence[AsyncIterable[Execution]]):
        self._logger = logger
        self._iterables = upstreams

    async def __aiter__(self) -> AsyncIterator[Execution]:
        iter_final: Optional[Execution] = None

        for iterable in self._iterables:

            execution: Optional[Execution] = None

            async for execution in iterable:
                if iter_final:
                    if execution.timestamp < iter_final.timestamp:
                        raise ValueError(f'Time stamp order is not ascend. (last: {iter_final}, this: {execution})')
                    iter_final = None

                yield execution
            else:
                iter_final = execution
