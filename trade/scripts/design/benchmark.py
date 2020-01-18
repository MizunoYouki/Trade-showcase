import asyncio
import sqlite3
import time
from argparse import ArgumentParser
from datetime import datetime
from typing import AsyncIterator, AsyncIterable, Iterable, Iterator

from trade.execution.model import Execution
from trade.model import Symbol

"""
# イテレータによるパイプラインか？ ジェネレータによるパイプラインか？

## 速度の検証
背景：プレイバックは、最速で行いたい
条件：80万レコードの bitFlyer_FXBTCJPY_828318090-2019-02-18T123750.991236700_829128533-2019-02-18T160337.290534600.sqlite3

同期イテレータ、非同期イテレータ、ジェネレータをによる各パイプラインで速度の差は無かった。

## パイプラインの各processorはイテレータか？ ジェネレータか？
- イテレータ
  - pros
    - ひとつのprocessorが、複数のupstreamイテレータから値を取得できる
    - 入力と出力で数が一致しなくても自然だ
  - cons
    - パイプラインの各プロセッサーは、非同期イテレータか同期イテレータどちらかに統一する必要がある。混ぜて使えない。
      → 非同期イテレータに統一して問題ない、と判断した。

- ジェネレータ
  - pros
    - 入力に対応する出力が必ずあるので、テストが見やすい
  - cons
    - 複数の入力からsendできるが、送信元を判別するならsendされた側で行う必要がある
    - 入力と出力の数が一致しない場合Noneを返すので、利用者はNoneチェックが必要
"""


async def playback_iter(path: str, benchmark=False):
    """
    benchmark=Trueで、3.3 sec（playback_async_iterと比べて、リスト内包表記にできた箇所の分だけ速い）
    """

    class SQLiteExecutionIterable(Iterable[Execution]):

        def __init__(self, symbol: Symbol, conn: sqlite3.Connection):
            self._symbol = symbol
            self._conn = conn
            self._conn.row_factory = sqlite3.Row

        def __iter__(self) -> Iterator[Execution]:
            with self._conn:
                yield from [row for row in
                            self._conn.execute('SELECT id, side, price, size, exec_date, buy_child_order_acceptance_id,'
                                               'sell_child_order_acceptance_id FROM executions ORDER BY id')]

    class IterStream(Iterable[Execution]):

        def __init__(self, upstream: Iterable[Execution]):
            self._upstream = upstream

        def __iter__(self) -> Iterator[Execution]:
            if benchmark:
                yield from self._upstream

            else:
                for e in self._upstream:
                    print(f'#{id(self)} < {e}')

                    if e['id'] % 3 == 0:
                        print(f'async sleep: {datetime.now()}')

                        time.sleep(1)

                    yield e

    conn = sqlite3.connect(path)
    subscriber = SQLiteExecutionIterable(symbol=Symbol.FXBTCJPY, conn=conn)
    s1 = IterStream(upstream=subscriber)
    s2 = IterStream(upstream=s1)

    t = time.monotonic()
    for e in s2:
        if not benchmark:
            print(f'< {e}')
            print('-' * 20)
    print(time.monotonic() - t, e['id'])


async def playback_async_iter(path: str, benchmark=False):
    """
    benchmark=Trueで、3.5 sec
    """

    class AsyncSQLiteExecutionIterable(AsyncIterable[Execution]):

        def __init__(self, symbol: Symbol, conn: sqlite3.Connection):
            self._symbol = symbol
            self._conn = conn
            self._conn.row_factory = sqlite3.Row

        async def __aiter__(self) -> AsyncIterator[Execution]:
            with self._conn:
                for row in self._conn.execute('SELECT id, side, price, size, exec_date, buy_child_order_acceptance_id,'
                                              'sell_child_order_acceptance_id FROM executions ORDER BY id'):
                    # yield from ... はasync関数内なので、使用不可

                    yield row

    class AsyncIterStream(AsyncIterable[Execution]):

        def __init__(self, upstream: AsyncIterable[Execution]):
            self._upstream = upstream

        async def __aiter__(self) -> AsyncIterator[Execution]:
            async for e in self._upstream:
                print(f'#{id(self)} < {e}')

                if e['id'] % 3 == 0:
                    print(f'async sleep: {datetime.now()}')

                    time.sleep(1)

                yield e

    class BenchmarkAsyncIterStream(AsyncIterable[Execution]):

        def __init__(self, upstream: AsyncIterable[Execution]):
            self._upstream = upstream

        async def __aiter__(self) -> AsyncIterator[Execution]:
            async for e in self._upstream:
                yield e

    conn = sqlite3.connect(path)
    subscriber = AsyncSQLiteExecutionIterable(symbol=Symbol.FXBTCJPY, conn=conn)
    s1 = benchmark and BenchmarkAsyncIterStream(upstream=subscriber) or AsyncIterStream(upstream=subscriber)
    s2 = benchmark and BenchmarkAsyncIterStream(upstream=s1) or AsyncIterStream(upstream=s1)

    t = time.monotonic()
    async for e in s2:
        if not benchmark:
            print(f'< {e}')
            print('-' * 20)
    print(time.monotonic() - t, e['id'])  # 829128533


async def realtime_async_iter():
    class AsyncExecutionIterator:

        def __init__(self):
            self._n = 0

        async def __aiter__(self) -> AsyncIterator[Execution]:
            while True:
                dt = datetime.now()
                print(f'born: {dt}')

                yield f'{self._n} - {dt}'

                self._n += 1

    class AsyncIterStream(AsyncIterable[Execution]):

        def __init__(self, upstream: AsyncIterable[Execution]):
            self._upstream = upstream

        async def __aiter__(self) -> AsyncIterator[Execution]:
            async for e in self._upstream:
                print(f'#{id(self)} < {e}')

                n = int(e.split()[0])
                if n and n % 3 == 0:
                    print('async sleep')

                    time.sleep(1)

                yield e

    subscriber = AsyncExecutionIterator()
    s1 = AsyncIterStream(upstream=subscriber)
    s2 = AsyncIterStream(upstream=s1)

    async for e in s2:
        print(f'< {e}')
        print('-' * 20)


if __name__ == '__main__':
    p = ArgumentParser()
    p.add_argument('--playback-iter', action='store_true')
    p.add_argument('--playback-async-iter', action='store_true')
    p.add_argument('--realtime-async-iter', action='store_true')
    p.add_argument('--benchmark', action='store_true')
    p.add_argument('--sqlite-path')
    args = p.parse_args()

    if args.playback_iter:
        asyncio.run(playback_iter(args.sqlite_path, args.benchmark))

    elif args.playback_async_iter:
        asyncio.run(playback_async_iter(args.sqlite_path, args.benchmark))

    elif args.realtime_async_iter:
        asyncio.run(realtime_async_iter())
