import asyncio
import sys
from argparse import ArgumentParser
from functools import partial
from json import dumps, loads
from logging import Logger
from typing import Dict, Any

import websockets

from trade.execution.model import encode_bitflyer_channel, Execution, SwitchedToRealtime
from trade.execution.queue import TimeWindowExecutionQueue
from trade.log import get_logger
from trade.model import Symbol


class WarmUpExecutionWebSocketProxyServer:
    """
    保持期間付きの、約定配信WebSocketプロキシサーバ

    `RealtimeWebSocketStream`が購読できるJSON形式で配信します。
    """

    _q: TimeWindowExecutionQueue

    def __init__(self,
                 logger: Logger,
                 warm_up_window: str,
                 switched_to_realtime_partial: partial,
                 host='localhost',
                 port=8765):
        self._logger = logger
        self._warm_up_window = warm_up_window
        self._host = host
        self._port = port
        self._q: 'TimeWindowExecutionQueue[Execution]' = TimeWindowExecutionQueue(
            logger=self._logger,
            time_window=self._warm_up_window,
            switched_to_realtime_partial=switched_to_realtime_partial
        )

    def start(self):
        asyncio.run(self._start())

    async def _start(self):
        self._logger.info(f'starting server: ws://{self._host}:{self._port}')

        await asyncio.gather(
            asyncio.create_task(self._proxying()),
            websockets.serve(self._handle_client, self._host, self._port),
        )

    async def _proxying(self):
        uri = 'wss://ws.lightstream.bitflyer.com/json-rpc'

        self._logger.info(f'started to receive from upstream: {uri}')
        self._logger.info(f'warming up window: {self._warm_up_window}')

        async with websockets.connect(uri) as ws_upstream:
            for channel in ['lightning_executions_BTC_JPY', 'lightning_executions_FX_BTC_JPY']:
                await ws_upstream.send(dumps({'method': 'subscribe', 'params': {'channel': channel}}))
                self._logger.info(f'> subscribe channel: {channel}')

            raw_response: str
            async for raw_response in ws_upstream:
                response: Dict[str, Any] = loads(raw_response)
                params: Dict[str, Any] = response['params']
                symbol: Symbol = encode_bitflyer_channel(params['channel'])
                for message in params['message']:
                    message['channel'] = params['channel']
                    message['raw_response'] = dumps(message)
                    execution = Execution.encode_bitflyer_response_raw(symbol, message)
                    self._q.put_nowait(execution)

    async def _handle_client(self, ws: websockets.WebSocketServerProtocol, path: str):
        self._logger.info('started to handle client')

        client_key = ws.request_headers['Sec-WebSocket-Key']
        self._logger.info(f'got client key: {client_key}')

        self._q.spawn_queue(client_key)
        self._logger.info(f'spawned queue for client: {client_key}, n-execution: {self._q.execution_count()}')

        while True:
            execution = await self._q.get(client_key)

            if isinstance(execution, SwitchedToRealtime):
                await ws.send(repr(execution))
                continue

            if 'raw_response' in execution.attrs:
                try:
                    await ws.send(execution.attrs['raw_response'])

                except websockets.exceptions.ConnectionClosedError as e:
                    self._logger.info(
                        f'could not send execution to the client, disposing spawned queue...: {client_key}'
                    )

                    self._q.dispose_queue(client_key)
                    self._logger.info(f'successfully finished to dispose spawned queue: {client_key}')
                    self._logger.info(f'number of remaining spawned queues: {self._q.spawned_queue_count()}')

                    return


if __name__ == '__main__':
    _p = ArgumentParser()
    _p.add_argument('--warm-up-window')
    _p.add_argument('--symbol')
    _p.add_argument('--host')
    _p.add_argument('--port')
    _args = _p.parse_args()

    _logger = get_logger(__name__, _format='%(asctime)s:%(levelname)s:%(message)s', stream=sys.stdout)

    distributor = WarmUpExecutionWebSocketProxyServer(
        logger=get_logger(__name__, stream=sys.stdout, level='INFO', _format='%(asctime)s:%(levelname)s:%(message)s'),
        warm_up_window=_args.warm_up_window,
        switched_to_realtime_partial=partial(SwitchedToRealtime, symbol=Symbol(_args.symbol)),
        host=_args.host,
        port=_args.port,
    )
    distributor.start()
