import asyncio
import logging
import time
from json import loads
from typing import Dict, TypeVar, Callable

from aiohttp import ClientSession
from tenacity import before_sleep_log, retry, wait_combine, TryAgain

from trade.log import LoggingAdapter

T = TypeVar('T')


class _TryAgain(TryAgain):
    pass


class _TryAgain429(TryAgain):
    pass


class HTTPClient:
    class Wait(wait_combine):

        def __init__(self, time_wait_retrying: float, time_wait_429_suspends: int, *strategies):
            super().__init__(*strategies)

            self._time_wait_429_suspends = time_wait_429_suspends
            self._time_wait_retrying = time_wait_retrying

        def __call__(self, retry_state):
            exception = retry_state.outcome.exception()
            if isinstance(exception, _TryAgain):
                return self._time_wait_retrying

            elif isinstance(exception, _TryAgain429):
                return self._time_wait_429_suspends

            else:
                raise TypeError(f'Unexpected exception: {type(exception)}: {exception}')

    def __init__(self, logger: logging.Logger,
                 client_session: ClientSession,
                 time_wait_retrying: float,
                 time_wait_429_suspends: int = 300):

        @retry(wait=HTTPClient.Wait(time_wait_429_suspends=time_wait_429_suspends,
                                    time_wait_retrying=time_wait_retrying),
               before_sleep=before_sleep_log(logger, logging.WARNING))
        async def _opener(_id: str,
                          started: float,
                          method: str,
                          url: str,
                          query: Dict[str, str],
                          headers: Dict[str, str],
                          post_data: str = '') -> [int, dict]:
            _logger = LoggingAdapter(self._logger, {'class': self.__class__.__name__, 'method': self._opener.__name__})

            if method == 'GET':
                _logger.info(f'@{_id} started to send GET request: (url: {url}, query: {query})')

                async def get(_session, _url, _params, _headers) -> [int, dict]:
                    async with _session.get(_url, params=_params, headers=_headers) as response:
                        _logger.debug(f'@{_id} response: {response}')

                        _json_response = await response.json()
                        elapsed = time.time() - started
                        _logger.info(f'@{_id} finished to send request: (elapsed: {elapsed} s)')

                        return response.status, _json_response

                _code, _json_dict = await get(client_session, url, query, headers)
                if _code != 200:
                    _logger.warning(f'@{_id} code: {_code}, json_dict: {_json_dict}')
                    if _code == 429:
                        raise _TryAgain429(f'@{_id}')
                    raise _TryAgain(f'@{_id}')

                return _code, _json_dict

            elif method == 'POST':
                _logger.info(f'@{_id} started to send POST request: (url: {url}, data: {post_data})')

                async def post(_session, _url, _params, _headers, post_body) -> [int, dict]:
                    post_body = post_body.encode('utf-8')

                    async with _session.post(_url, params=_params, headers=_headers, data=post_body) as response:
                        _logger.debug(f'@{_id} response: {response}')

                        elapsed = time.time() - started
                        _logger.info(f'@{_id} finished to send request: (elapsed: {elapsed} s)')

                        _response_json = await response.text()
                        if not _response_json:
                            return response.status, dict()

                        return response.status, loads(_response_json)

                _code, _json_dict = await post(client_session, url, query, headers, post_data)
                if _code != 200:
                    _logger.warning(f'@{_id} code: {_code}, json_dict: {_json_dict}')
                    if _code == 429:
                        raise _TryAgain429(f'@{_id}')
                    raise _TryAgain(f'@{_id}')

                return _code, _json_dict

            else:
                raise Exception(f'Unsupported method: {method}')

        self._logger = logger
        self._opener = _opener
        self.time_wait_429_suspends = time_wait_429_suspends
        self.time_wait_retrying = time_wait_retrying

    async def send_request(
            self,
            _id: str,
            response_mapper: Callable[..., T],
            method: str,
            url: str,
            headers: Dict[str, str],
            query: Dict[str, str] = None,
            post_data: str = '') -> [int, T]:

        started = time.time()

        if not query:
            query = {}

        logger = LoggingAdapter(self._logger, {'class': self.__class__.__name__, 'method': self.send_request.__name__})

        try:
            code, json_dict = await self._opener(_id=_id, started=started, method=method, url=url, query=query,
                                                 headers=headers, post_data=post_data)
            logger.debug(f'@{_id} code: {code}, json_dict: {json_dict}')

        except asyncio.CancelledError:
            logger.info(f'@{_id} caught CE, aborted')
            raise

        mapped = response_mapper(json_dict)
        logger.debug(f'@{_id} mapped: {mapped}')

        return code, mapped
