import asyncio
import collections
from logging import Logger


class Queue(asyncio.Queue):

    def __init__(self, logger: Logger, maxsize=0, *, loop=None):
        super().__init__(maxsize, loop=loop)

        self._logger = logger

    def _init(self, maxsize):
        self._queue = collections.deque()

    def clear(self):
        while self._queue:
            v = self._queue.popleft()
            self._logger.info(f'dispose: {v}')


class LifoQueue(asyncio.LifoQueue):

    def __init__(self, logger: Logger, maxsize=0, *, loop=None):
        super().__init__(maxsize, loop=loop)

        self._logger = logger

    def _init(self, maxsize):
        self._queue = []

    def clear(self):
        while self._queue:
            v = self._queue.pop()
            self._logger.info(f'dispose: {v}')
