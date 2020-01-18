import asyncio
import unittest
from asyncio import Task
from dataclasses import dataclass
from decimal import Decimal
from functools import partial
from logging import Logger
from typing import List, Dict, Union, Tuple, Mapping

from aiohttp import ClientSession, ClientResponse
from aioresponses import aioresponses
from aioresponses.compat import normalize_url, merge_params
from yarl import URL

from trade.broker.declarative.bitflyer import BitflyerBroker
from trade.broker.declarative.model import Position, NormalizedPositions
from trade.broker.declarative.queue import LifoQueue as LifoQueueClearable
from trade.broker.httpclient import HTTPClient
from trade.log import get_logger
from trade.model import Symbol
from trade.side import Side


@dataclass
class GetRecorded:
    url: str


@dataclass
class PostRecorded:
    url: str
    data: object


class BitflyerBrokerTestCase(unittest.IsolatedAsyncioTestCase):

    def _url(self, part):
        return 'https://api.bitflyer.com' + part

    class AwaiterNormalizedPositions(NormalizedPositions):
        def __init__(self, positions: Mapping[Symbol, Position], await_after_putted: float = 0.0):
            super().__init__(positions)

            self.await_after_putted = await_after_putted

    class RecordableAioResponses(aioresponses):

        def __init__(self, **kwargs):
            super().__init__(**kwargs)

            self.requests_recorded: List[Union[GetRecorded, PostRecorded]] = list()

        async def _request_mock(self, orig_self: ClientSession,
                                method: str, url: Union[URL, str],
                                *args: Tuple,
                                **kwargs: Dict) -> ClientResponse:
            if method == 'GET':
                full_url = normalize_url(merge_params(url, kwargs.get('params')))
                self.requests_recorded.append(GetRecorded(url=str(full_url)))
            elif method == 'POST':
                data = kwargs['data']
                self.requests_recorded.append(PostRecorded(url=url, data=data))

            return await super()._request_mock(orig_self, method, url, *args, **kwargs)

        @staticmethod
        def build_requests_expected(assertive_mocks) -> List[Union[GetRecorded, PostRecorded]]:
            expected: List[Union[GetRecorded, PostRecorded]] = list()
            for assertive_mock in assertive_mocks:
                if assertive_mock['response'].func.__func__.__name__ == 'get':
                    expected.append(GetRecorded(
                        url=assertive_mock['response'].args[0]
                    ))
                elif assertive_mock['response'].func.__func__.__name__ == 'post':
                    expected.append(PostRecorded(
                        url=assertive_mock['response'].args[0], data=assertive_mock['expected']['data']
                    ))
            return expected

    def _build_broker(self, logger: Logger,
                      http_client: HTTPClient,
                      positions_queue: 'LifoQueueClearable[NormalizedPositions]',
                      delay: float = 0):
        return BitflyerBroker.create_broker(
            positions_queue, logger, http_client=http_client,
            api_key='dummy-api-key', api_secret='dummy-api-secret', delay=delay
        )

    def _build_tasks(self, broker: BitflyerBroker) -> Tuple[Task, Task, Task]:
        trader_task: Task = asyncio.create_task(broker.trader())
        start_new_trader_task: Task = asyncio.create_task(broker.start_new_trader(trader_task))
        observer_task: Task = asyncio.create_task(broker.observer())
        return trader_task, observer_task, start_new_trader_task

    def _assert_graceful_done(self, task: Task):
        self.assertTrue(task.done(), f'Expected that task:{task} is done')
        self.assertFalse(task.cancelled(), f'Expected that task:{task} is not cancelled')
        self.assertIsNone(task.exception(), f'Expected that task:{task} does not raise, stack:{task.get_stack()}')

    def _assert_cancelled(self, task: Task):
        self.assertTrue(task.done(), f'Expected that task:{task} is done')
        self.assertTrue(task.cancelled(), f'Expected that task:{task} is cancelled')

    def _assert_exception(self, task: Task):
        self.assertTrue(task.done(), f'Expected that task:{task} is done')
        self.assertTrue(task.exception(), f'Expected that task:{task} raises')

    def _assert_running(self, task: Task):
        self.assertFalse(task.done(), f'Expected that task:{task} is not done')
        self.assertFalse(task.cancelled(), f'Expected that task:{task} is not cancelled')

    class MockStrategies:
        def __init__(self, logger: Logger,
                     positions_queue: 'LifoQueueClearable[NormalizedPositions]',
                     positions: 'List[BitflyerBrokerTestCase.AwaiterNormalizedPositions]',
                     timeout: float):
            self._logger = logger
            self._positions_queue = positions_queue
            self.positions = positions
            self.timeout = timeout

        async def positions_distributor(self):
            while True:
                if not self.positions:
                    return

                position: 'BitflyerBrokerTestCase.AwaiterNormalizedPositions' = self.positions.pop(0)
                self._positions_queue.put_nowait(position)
                if position.await_after_putted:
                    await asyncio.sleep(position.await_after_putted)

    async def asyncSetUp(self) -> None:
        self._client_session: ClientSession = ClientSession()

    async def asyncTearDown(self) -> None:
        await self._client_session.close()

    @RecordableAioResponses()
    async def test_common(self, m: RecordableAioResponses):
        assertive_mocks = [
            {'response': partial(
                m.get,
                self._url('/v1/me/getchildorders?child_order_state=ACTIVE&product_code=FX_BTC_JPY'),
                status=200,
                payload=[],
            )},
            {'response': partial(
                m.get,
                self._url('/v1/me/getpositions?product_code=FX_BTC_JPY'),
                status=200,
                payload=[],
            )},
            {
                'response': partial(m.post, self._url('/v1/me/sendchildorder'), status=200,
                                    payload={'child_order_acceptance_id': 'JRF20190923-151623-780630'}),
                'expected': {
                    'data': b'{"product_code": "FX_BTC_JPY", "child_order_type": "LIMIT", "side": "BUY", '
                            b'"size": 1.0, "price": 1, "minute_to_expire": 43200, "time_in_force": "GTC"}',
                },
            },
            {'response': partial(
                m.get,
                self._url('/v1/me/getchildorders'
                          '?child_order_acceptance_id=JRF20190923-151623-780630'
                          '&child_order_state=COMPLETED'
                          '&product_code=FX_BTC_JPY'),
                status=200,
                payload=[],
            )},
            {'response': partial(
                m.get,
                self._url('/v1/me/getchildorders'
                          '?child_order_acceptance_id=JRF20190923-151623-780630'
                          '&child_order_state=ACTIVE'
                          '&product_code=FX_BTC_JPY'),
                status=200,
                payload=[{
                    "id": 138400,
                    "child_order_id": "JOR20150707-084555-022522",
                    "child_order_acceptance_id": "JRF20190923-151623-780630",
                    "product_code": "FX_BTC_JPY",
                    "side": "SELL",
                    "child_order_type": "LIMIT",
                    "price": 1,
                    "size": 1.0,
                    "child_order_state": "COMPLETED",
                }],
            )},
        ]
        [assertive_mock['response']() for assertive_mock in assertive_mocks]  # setup mock responses

        logger = get_logger(self.test_common.__name__, _format='%(asctime)s: %(levelname)s:%(message)s')
        positions_queue: 'LifoQueueClearable[BitflyerBrokerTestCase.AwaiterNormalizedPositions]' \
            = LifoQueueClearable(logger)
        strategies = self.MockStrategies(logger, positions_queue, positions=[
            self.AwaiterNormalizedPositions({
                Symbol.FXBTCJPY: Position(
                    symbol=Symbol.FXBTCJPY, side=Side.BUY, price=Decimal('1'), size=Decimal('1')
                )
            })
        ], timeout=0.1)
        broker = await self._build_broker(
            logger,
            HTTPClient(
                logger=logger, client_session=self._client_session, time_wait_retrying=0, time_wait_429_suspends=0
            ),
            positions_queue
        )
        positions_distributor_task: Task = asyncio.create_task(strategies.positions_distributor())
        trader_task, observer_task, start_new_trader_task = self._build_tasks(broker)

        await asyncio.wait(
            [
                positions_distributor_task,
                start_new_trader_task,
                observer_task,
                trader_task,
            ],
            return_when=asyncio.ALL_COMPLETED,
            timeout=strategies.timeout,
        )
        requests_expected = self.RecordableAioResponses.build_requests_expected(assertive_mocks)
        self.assertEqual(len(requests_expected), len(m.requests_recorded))
        for n, e in enumerate(requests_expected):
            self.assertEqual(e, m.requests_recorded[n], n)

        self.assertEqual(0, broker.candidate_qsize())
        self.assertEqual(0, broker.newest_qsize())
        self.assertEqual(0, len(broker.trader_tasks_started_later))

        self._assert_graceful_done(positions_distributor_task)
        self._assert_running(trader_task)
        self._assert_running(start_new_trader_task)
        self._assert_running(observer_task)

    @RecordableAioResponses()
    async def test_retry(self, m: RecordableAioResponses):
        assertive_mocks = [
            {'response': partial(
                m.get,
                self._url('/v1/me/getchildorders'
                          '?child_order_state=ACTIVE'
                          '&product_code=FX_BTC_JPY'),
                status=200,
                payload=[{
                    "id": 138399,
                    "child_order_id": "JOR20150707-084555-022521",
                    "child_order_acceptance_id": "JRF20190923-151623-780629",
                    "product_code": "FX_BTC_JPY",
                    "side": "BUY",
                    "child_order_type": "LIMIT",
                    "price": 1,
                    "size": 1.0,
                    "child_order_state": "ACTIVE",
                }],
            )},
            {
                'response': partial(m.post, self._url('/v1/me/cancelchildorder'), status=200, payload=[]),
                'expected': {
                    'data': b'{"product_code": "FX_BTC_JPY", "child_order_id": "JOR20150707-084555-022521"}',
                },
            },

            # Fire retrying (Could not confirmed that child order(s) cancelled)
            {'response': partial(
                m.get,
                self._url('/v1/me/getchildorders'
                          '?child_order_state=ACTIVE'
                          '&product_code=FX_BTC_JPY'),
                status=200,
                payload=[{
                    "id": 138399,
                    "child_order_id": "JOR20150707-084555-022521",
                    "child_order_acceptance_id": "JRF20190923-151623-780629",
                    "product_code": "FX_BTC_JPY",
                    "side": "BUY",
                    "child_order_type": "LIMIT",
                    "price": 1,
                    "size": 1.0,
                    "child_order_state": "ACTIVE",
                }],
            )},

            {'response': partial(
                m.get,
                self._url('/v1/me/getchildorders'
                          '?child_order_state=ACTIVE'
                          '&product_code=FX_BTC_JPY'),
                status=200,
                payload=[],
            )},

            # Fire retrying (HTTP status code 500)
            {'response': partial(
                m.get,
                self._url('/v1/me/getpositions?product_code=FX_BTC_JPY'),
                status=500,
                payload=dict(Message='An error has occurred.')
            )},

            {'response': partial(
                m.get,
                self._url('/v1/me/getpositions?product_code=FX_BTC_JPY'),
                status=200,
                payload=[],
            )},

            # Fire retrying (HTTP status code 500)
            {
                'response': partial(m.post, self._url('/v1/me/sendchildorder'), status=500,
                                    payload=dict(Message='An error has occurred again.')),
                'expected': {
                    'data': b'{"product_code": "FX_BTC_JPY", "child_order_type": "LIMIT", "side": "BUY", '
                            b'"size": 1.0, "price": 1, "minute_to_expire": 43200, "time_in_force": "GTC"}',
                },
            },

            # Fire retrying (HTTP status code 502)
            {
                'response': partial(m.post, self._url('/v1/me/sendchildorder'), status=502,
                                    payload=dict(Message='Mock bad gateway.')),
                'expected': {
                    'data': b'{"product_code": "FX_BTC_JPY", "child_order_type": "LIMIT", "side": "BUY", '
                            b'"size": 1.0, "price": 1, "minute_to_expire": 43200, "time_in_force": "GTC"}',
                },
            },

            {
                'response': partial(m.post, self._url('/v1/me/sendchildorder'), status=200,
                                    payload={'child_order_acceptance_id': 'JRF20190923-151623-780630'}),
                'expected': {
                    'data': b'{"product_code": "FX_BTC_JPY", "child_order_type": "LIMIT", "side": "BUY", '
                            b'"size": 1.0, "price": 1, "minute_to_expire": 43200, "time_in_force": "GTC"}',
                },
            },

            # Fire retrying (HTTP status code 500)
            {'response': partial(
                m.get,
                self._url('/v1/me/getchildorders'
                          '?child_order_acceptance_id=JRF20190923-151623-780630'
                          '&child_order_state=COMPLETED'
                          '&product_code=FX_BTC_JPY'),
                status=500,
                payload=dict(Message='An error has occurred again/again.'),
            )},

            {'response': partial(
                m.get,
                self._url('/v1/me/getchildorders'
                          '?child_order_acceptance_id=JRF20190923-151623-780630'
                          '&child_order_state=COMPLETED'
                          '&product_code=FX_BTC_JPY'),
                status=200,
                payload=[{
                    "id": 138400,
                    "child_order_id": "JOR20150707-084555-022522",
                    "child_order_acceptance_id": "JRF20190923-151623-780630",
                    "product_code": "FX_BTC_JPY",
                    "side": "SELL",
                    "child_order_type": "LIMIT",
                    "price": 1,
                    "size": 1.0,
                    "child_order_state": "ACTIVE",
                }],
            )},
        ]
        [assertive_mock['response']() for assertive_mock in assertive_mocks]  # setup mock responses

        logger = get_logger(self.test_retry.__name__, _format='%(asctime)s: %(levelname)s:%(message)s')
        positions_queue: 'LifoQueueClearable[BitflyerBrokerTestCase.AwaiterNormalizedPositions]' \
            = LifoQueueClearable(logger)
        strategies = self.MockStrategies(logger, positions_queue, positions=[
            self.AwaiterNormalizedPositions({
                Symbol.FXBTCJPY: Position(
                    symbol=Symbol.FXBTCJPY, side=Side.BUY, price=Decimal('1'), size=Decimal('1')
                )
            })
        ], timeout=0.1)
        broker = await self._build_broker(
            logger,
            HTTPClient(
                logger=logger, client_session=self._client_session, time_wait_retrying=0, time_wait_429_suspends=0
            ),
            positions_queue
        )
        positions_distributor_task: Task = asyncio.create_task(strategies.positions_distributor())
        trader_task, observer_task, start_new_trader_task = self._build_tasks(broker)

        await asyncio.wait(
            [
                positions_distributor_task,
                start_new_trader_task,
                observer_task,
                trader_task,
            ],
            return_when=asyncio.ALL_COMPLETED,
            timeout=strategies.timeout
        )
        requests_expected = self.RecordableAioResponses.build_requests_expected(assertive_mocks)
        self.assertEqual(len(requests_expected), len(m.requests_recorded))
        for n, e in enumerate(requests_expected):
            self.assertEqual(e, m.requests_recorded[n])

        self.assertEqual(0, broker.candidate_qsize())
        self.assertEqual(0, broker.newest_qsize())
        self.assertEqual(0, len(broker.trader_tasks_started_later))

        self._assert_graceful_done(positions_distributor_task)
        self._assert_running(trader_task)
        self._assert_running(start_new_trader_task)
        self._assert_running(observer_task)

    @RecordableAioResponses()
    async def test_ood(self, m: RecordableAioResponses):
        assertive_mocks = [
            {'response': partial(
                m.get,
                self._url('/v1/me/getchildorders?child_order_state=ACTIVE&product_code=FX_BTC_JPY'),
                status=200,
                payload=[],
            )},
            {'response': partial(
                m.get,
                self._url('/v1/me/getpositions?product_code=FX_BTC_JPY'),
                status=200,
                payload=[],
            )},
            {
                'response': partial(m.post, self._url('/v1/me/sendchildorder'), status=200,
                                    payload={'child_order_acceptance_id': 'JRF20190923-151623-780630'}),
                'expected': {
                    'data': b'{"product_code": "FX_BTC_JPY", "child_order_type": "LIMIT", "side": "BUY", '
                            b'"size": 1.0, "price": 1, "minute_to_expire": 43200, "time_in_force": "GTC"}',
                },
            },

            # Here, OOD

            {'response': partial(
                m.get,
                self._url('/v1/me/getchildorders'
                          '?child_order_state=ACTIVE'
                          '&product_code=FX_BTC_JPY'),
                status=200,
                payload=[{
                    "id": 138401,
                    "child_order_id": "JOR20150707-084555-022521",
                    "child_order_acceptance_id": "JRF20190923-151623-780631",
                    "product_code": "FX_BTC_JPY",
                    "side": "BUY",
                    "child_order_type": "LIMIT",
                    "price": 1,
                    "size": 1.0,
                    "child_order_state": "ACTIVE",
                }],
            )},
            {
                'response': partial(m.post, self._url('/v1/me/cancelchildorder'), status=200, payload=[]),
                'expected': {
                    'data': b'{"product_code": "FX_BTC_JPY", "child_order_id": "JOR20150707-084555-022521"}',
                },
            },

            # Here, OOD

            {'response': partial(
                m.get,
                self._url('/v1/me/getchildorders?child_order_state=ACTIVE&product_code=FX_BTC_JPY'),
                status=200,
                payload=[],
            )},
            {'response': partial(
                m.get,
                self._url('/v1/me/getpositions?product_code=FX_BTC_JPY'),
                status=200,
                payload=[],
            )},
            {
                'response': partial(m.post, self._url('/v1/me/sendchildorder'), status=200,
                                    payload={'child_order_acceptance_id': 'JRF20190923-151623-780633'}),
                'expected': {
                    'data': b'{"product_code": "FX_BTC_JPY", "child_order_type": "LIMIT", "side": "BUY", '
                            b'"size": 3.0, "price": 3, "minute_to_expire": 43200, "time_in_force": "GTC"}',
                },
            },
            {'response': partial(
                m.get,
                self._url('/v1/me/getchildorders'
                          '?child_order_acceptance_id=JRF20190923-151623-780633'
                          '&child_order_state=COMPLETED'
                          '&product_code=FX_BTC_JPY'),
                status=200,
                payload=[{
                    "id": 138403,
                    "child_order_id": "JOR20150707-084555-022523",
                    "child_order_acceptance_id": "JRF20190923-151623-780633",
                    "product_code": "FX_BTC_JPY",
                    "side": "SELL",
                    "child_order_type": "LIMIT",
                    "price": 3,
                    "size": 3.0,
                    "child_order_state": "ACTIVE",
                }],
            )},
        ]
        [assertive_mock['response']() for assertive_mock in assertive_mocks]  # setup mock responses

        logger = get_logger(self.test_ood.__name__, _format='%(asctime)s: %(levelname)s:%(message)s')
        positions_queue: 'LifoQueueClearable[BitflyerBrokerTestCase.AwaiterNormalizedPositions]' \
            = LifoQueueClearable(logger)
        strategies = self.MockStrategies(logger, positions_queue, positions=[
            self.AwaiterNormalizedPositions({Symbol.FXBTCJPY: Position(
                symbol=Symbol.FXBTCJPY, side=Side.BUY, price=Decimal('1'), size=Decimal('1')
            )}, await_after_putted=0.01),
            self.AwaiterNormalizedPositions({Symbol.FXBTCJPY: Position(
                symbol=Symbol.FXBTCJPY, side=Side.BUY, price=Decimal('2'), size=Decimal('2')
            )}, await_after_putted=0.01),
            self.AwaiterNormalizedPositions({Symbol.FXBTCJPY: Position(
                symbol=Symbol.FXBTCJPY, side=Side.BUY, price=Decimal('3'), size=Decimal('3')
            )}),
        ], timeout=(0.01 + 0.1) + (0.01 + 0.1) + (0.01 + 0.1))
        broker = await self._build_broker(
            logger,
            HTTPClient(
                logger=logger, client_session=self._client_session, time_wait_retrying=0, time_wait_429_suspends=0
            ),
            positions_queue,
            delay=0.1
        )
        position_distributor_task: Task = asyncio.create_task(strategies.positions_distributor())
        trader_task, observer_task, start_new_trader_task = self._build_tasks(broker)

        await asyncio.wait(
            [
                position_distributor_task,
                start_new_trader_task,
                observer_task,
                trader_task,
            ],
            return_when=asyncio.ALL_COMPLETED,
            timeout=strategies.timeout
        )
        requests_expected = self.RecordableAioResponses.build_requests_expected(assertive_mocks)
        self.assertEqual(len(requests_expected), len(m.requests_recorded))
        for n, e in enumerate(requests_expected):
            self.assertEqual(e, m.requests_recorded[n])

        self.assertEqual(0, broker.candidate_qsize())
        self.assertEqual(0, broker.newest_qsize())
        self.assertEqual(2, len(broker.trader_tasks_started_later))

        self._assert_graceful_done(position_distributor_task)
        self._assert_cancelled(trader_task)
        self._assert_running(start_new_trader_task)
        self._assert_running(observer_task)

    # TODO: Test for api limit reached
    # TODO: Test for HTTP code 400


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(BitflyerBrokerTestCase))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='test_suite', verbosity=2, buffer=True)
