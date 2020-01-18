from logging import Logger
from typing import Optional, AsyncIterable, AsyncIterator

from trade.execution.model import Execution, SynchronizedExecution


class SynchronizedStream(AsyncIterable[Execution]):
    """
    synchronized_executionではじまる名前の各種属性をセットする、Executionストリームアダプター


    Executionオブジェクトには、主の属性と、副の属性（synchronized_executionではじまる名前の属性）があります。

    プライマリ入力がイテレーションする値は、主の属性として全て出力されます。

    セカンダリ入力がイテレーションする値は、副の属性として出力されます。
    出力されるのは、プライマリオブジェクトに最も近傍なオブジェクトのみです。
    （`secondary.timestamp <= primary.timestamp`が保証されます）

    同じオブジェクトが、副の属性として複数回セットされることがあります。
    該当するオブジェクトが存在しない場合、副の各種属性は`None`になります。


    このクラスの利用者は、それぞれの入力イテレータがイテレーションする値のタイムスタンプが昇順であることに責務を持ちます。
    （この責務が果たされない場合、副の属性が正しくセットされません）


    プライマリまたはセカンダリ入力イテレータのいずれかが`StopIteration`例外を送出した場合、出来る限り長いペアを出力した後で、
    例外が送出されます。


    playback用のパイプprocessorなので、`SwitchedToRealtime`オブジェクトを受け取った時の動作は不定です。
    """

    def __init__(self, logger: Logger,
                 primary_iterable: AsyncIterable[Execution],
                 secondary_iterable: AsyncIterable[Execution]):
        self._logger = logger
        self._primary_iter = primary_iterable
        self._secondary_iter = secondary_iterable

    async def __aiter__(self) -> AsyncIterator[Execution]:
        primary_iter = self._primary_iter.__aiter__()
        if not primary_iter:
            return
        secondary_iter = self._secondary_iter.__aiter__()
        if not secondary_iter:
            return

        primary: Execution = await primary_iter.__anext__()
        secondary: SynchronizedExecution = SynchronizedExecution.from_execution(await secondary_iter.__anext__())

        prev_secondary: Optional[SynchronizedExecution] = None

        while secondary.timestamp <= primary.timestamp:
            prev_secondary = secondary
            secondary = SynchronizedExecution.from_execution(await secondary_iter.__anext__())

        while primary.timestamp < secondary.timestamp:
            yield Execution.wrap(execution=primary, synchronized_execution=prev_secondary)

            try:
                primary = await primary_iter.__anext__()
            except StopAsyncIteration:
                return

            while secondary.timestamp <= primary.timestamp:
                prev_secondary = secondary
                try:
                    secondary = SynchronizedExecution.from_execution(await secondary_iter.__anext__())
                except StopAsyncIteration:
                    if prev_secondary.timestamp <= primary.timestamp:
                        yield Execution.wrap(execution=primary, synchronized_execution=prev_secondary)
                    break
