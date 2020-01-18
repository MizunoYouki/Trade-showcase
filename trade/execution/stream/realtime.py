import asyncio
from json import loads
from logging import Logger
from typing import AsyncIterator, Dict, Any, Callable, Union, Mapping, AsyncIterable

import websockets

from trade.execution.model import SwitchedToRealtime, Execution
from trade.model import Symbol


class RealtimeWebSocketStream(AsyncIterable[Execution]):

    def __init__(self, logger: Logger,
                 uri: str,
                 symbol_resolver: Callable[[str], Symbol],
                 execution_encoder: Callable[[Symbol, Mapping[str, Union[str, int]]], Execution]):
        self._logger = logger
        self._uri = uri
        self._symbol_resolver = symbol_resolver
        self._execution_encoder = execution_encoder

    async def __aiter__(self) -> AsyncIterator[Execution]:
        async with websockets.connect(self._uri) as websocket:

            # noinspection PyUnresolvedReferences
            import numpy

            str_response: str

            async for str_response in websocket:
                if str_response.startswith(SwitchedToRealtime.__name__):
                    # self._logger.debug(f'< {str_response}')

                    switched_to_realtime = eval(str_response)
                    yield switched_to_realtime
                    continue

                message: Dict[str, Any] = loads(str_response)
                symbol: Symbol = self._symbol_resolver(message['channel'])
                execution = self._execution_encoder(symbol, message)
                # self._logger.debug(f'< {execution}')

                yield execution


if __name__ == '__main__':
    import sys

    from trade.log import get_logger
    from trade.execution.model import encode_bitflyer_channel

    reader = RealtimeWebSocketStream(
        logger=get_logger(__name__, stream=sys.stdout, level='DEBUG'),
        uri='ws://localhost:8765/',
        symbol_resolver=encode_bitflyer_channel,
        execution_encoder=Execution.encode_bitflyer_response
    )


    async def start():
        async for execution in reader:
            print(execution)


    asyncio.run(start(), debug=True)
