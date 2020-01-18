import asyncio
import hmac
import logging
import os
import random
import string
import sys
from asyncio import Task
from configparser import ConfigParser
from datetime import datetime
from decimal import Decimal
from enum import Enum, auto
from hashlib import sha256
from json import dumps
from logging import Logger
from traceback import format_exception
from typing import List, Tuple, Dict
from urllib.parse import urlencode, urlunparse
from urllib.request import Request

from tenacity import retry, before_sleep_log, TryAgain, wait_fixed

from trade import config
from trade.broker.declarative.bitflyer.model import BitflyerOrder, ChildOrder
from trade.broker.declarative.bitflyer.response import ChildOrders, BitflyerPositions
from trade.broker.declarative.model import Position, NormalizedPositions
from trade.broker.declarative.queue import LifoQueue as LifoQueueClearable
from trade.broker.httpclient import HTTPClient
from trade.broker.httpclient.response import BaseResponse
from trade.log import LoggingAdapter
from trade.model import Symbol


def inspect_tasks(logger):
    tasks = asyncio.all_tasks()
    for t in tasks:
        logger.debug(f'task: {t}')


class _UnexpectedResponseException(Exception):
    """
    予期しないレスポンス
    """


class BitflyerBroker:
    """
    冪等な、Bitflyer建玉の操作
    """

    class State(Enum):
        Idle = auto()
        Provisioning = auto()

    _state: State

    def __init__(self, logger: Logger,
                 http_client: HTTPClient,
                 api_key: str,
                 api_secret: str,
                 delay: float = 0.0):
        self._logger = logger
        self._http_client = http_client
        self._req_builder = BitflyerRequestBuilder(
            logger=logger, api_key=api_key, api_secret=api_secret.encode('utf-8')
        )
        self._delay = delay
        self._state = self.State.Idle
        self._hexdigits: str = string.digits + 'abcdef'
        self.trader_tasks_started_later: List[Task] = list()

    async def _init(self, logger: Logger, positions_queue: 'LifoQueueClearable[NormalizedPositions]'):
        self._candidate_queue = positions_queue
        self._newest_queue = LifoQueueClearable(logger)
        self._trader_to_be_cancelled = asyncio.Event()
        self._semaphore = asyncio.Semaphore()

    @staticmethod
    async def create_broker(positions_queue: 'LifoQueueClearable[NormalizedPositions]',
                            logger: Logger,
                            http_client: HTTPClient,
                            api_key: str,
                            api_secret: str,
                            delay: float = 0.0):
        broker = BitflyerBroker(logger, http_client, api_key=api_key, api_secret=api_secret, delay=delay)
        await broker._init(logger, positions_queue)
        return broker

    async def start_new_trader(self, trader_task: Task):
        logger = LoggingAdapter(self._logger,
                                {'class': self.__class__.__name__, 'method': self.start_new_trader.__name__})

        while True:
            logger.info(f'started to detect, running: {trader_task}')

            await self._trader_to_be_cancelled.wait()
            logger.info(f'detect that the trader should be cancelled: {trader_task}')

            async with self._semaphore:
                logger.info('exclusive...')

                trader_task.cancel()
                logger.info(f'finished to cancel the trader: {trader_task}')

                new_trader_task: Task = asyncio.create_task(self.trader())
                self.trader_tasks_started_later.append(new_trader_task)
                logger.info(f'finished to start the new trader: {new_trader_task}')

                trader_task = new_trader_task

                self._trader_to_be_cancelled.clear()

                logger.info('inclusive...')

    async def observer(self):
        logger = LoggingAdapter(self._logger, {'class': self.__class__.__name__, 'method': self.observer.__name__})

        while True:
            logger.info('waiting requirement')

            requirement: NormalizedPositions = await self._candidate_queue.get()
            if self._state is self.State.Idle:
                logger.info(f'got: {requirement}')

                self._state_to(self.State.Provisioning)
            else:
                logger.info(f'OOD, got: {requirement}')

                inspect_tasks(logger)
                self._trader_to_be_cancelled.set()

            self._newest_queue.put_nowait(requirement)

    async def trader(self):
        logger = LoggingAdapter(self._logger, {'class': self.__class__.__name__, 'method': self.trader.__name__})
        _id: str = ''

        while True:
            try:
                logger.info('waiting requirement')

                requirement: NormalizedPositions = await self._newest_queue.get()
                _id = self._generate_id()
                logger.info(f'@{_id} got: {requirement}')

                self._newest_queue.clear()

                await self._clearing_orders(_id)

                orders: List[BitflyerOrder] = await self._making_orders(_id, requirement)
                if all([order.size == Decimal('0') for order in orders]):
                    logger.info(f'@{_id} delta size is zero, nothing to order')

                    self._state_to(self.State.Idle)
                    continue

                await self._ordering(_id, orders)

                self._state_to(self.State.Idle)

            except Exception as err:
                tb = format_exception(type(err), err, sys.exc_info()[2])
                logger.error(f'@{_id} caught: {type(err)}: {err}, tb: {tb}')

                self._state_to(self.State.Idle)

                raise err

    def candidate_qsize(self):
        return self._candidate_queue.qsize()

    def newest_qsize(self):
        return self._newest_queue.qsize()

    def _generate_id(self):
        return ''.join(random.choice(self._hexdigits) for _ in range(8))

    def _inspect_tasks(self, logger):
        tasks = asyncio.all_tasks()
        for t in tasks:
            logger.info(f'task: {t}')

    async def _clearing_orders(self, _id: str):
        """
        state: ClearingOrders

        建玉および建玉に対応するストップロス注文のみになるよう、他の注文を削除中です。

        :param _id: 要求ID
        """
        logger = LoggingAdapter(self._logger,
                                {'class': self.__class__.__name__, 'method': self._clearing_orders.__name__})
        logger.info(f'@{_id} started')

        # ストップロスではない注文を直列にすべてキャンセルする
        #
        #   ここでは、"ストップロスではない注文" を "LIMIT" に限定する。
        #   このBrokerが行う、ストップロスではない注文がLIMITのみだからである。
        #
        # （注文の種類）
        # - "LIMIT": 指値注文。
        # - "MARKET" 成行注文。
        # - "STOP": ストップ注文。
        # - "STOP_LIMIT": ストップ・リミット注文。
        # - "TRAIL": トレーリング・ストップ注文。

        # TODO: required pagination ?
        _, active_child_orders = await self._get_child_orders(_id=_id, child_order_state='ACTIVE')
        active_child_orders: List[ChildOrder] = [o for o in active_child_orders if o.child_order_type == 'LIMIT']
        if active_child_orders:
            logger.info(f'@{_id} active child order: {[str(o) for o in active_child_orders]}')
        else:
            logger.info(f'@{_id} active child order does not exist')

        if active_child_orders:
            for child_order in active_child_orders:
                method = 'POST'
                path = '/v1/me/cancelchildorder'
                post_body = child_order.to_http_post_body_cancelchildorder()
                await self._http_client.send_request(
                    _id=_id,
                    response_mapper=lambda _: None,
                    method=method,
                    url=self._req_builder.build_url(path),
                    headers=self._req_builder.build_post_headers(method=method, path=path, post_body=post_body),
                    post_data=post_body,
                )

            # ACTIVEな注文がストップロス注文だけであることを保証する

            if self._delay:
                logger.info(f'wait {self._delay} second')
                await asyncio.sleep(self._delay)

            @retry(wait=wait_fixed(self._http_client.time_wait_retrying),
                   before_sleep=before_sleep_log(logger, logging.WARNING))
            async def _ensure_active_child_order_does_not_exist():
                code, aco = await self._get_child_orders(_id=_id, child_order_state='ACTIVE')
                aco = [o for o in aco if o.child_order_type == 'LIMIT']
                if aco:
                    logger.warning(f'@{_id} Could not confirmed that child order(s) cancelled'
                                   f', code: {code}, child_orders: {active_child_orders}')
                    raise TryAgain(f'@{_id}')

            await _ensure_active_child_order_does_not_exist()

        logger.info(f'@{_id} successfully finished')

    async def _making_orders(self, _id: str, requirement: NormalizedPositions) -> List[BitflyerOrder]:
        """
        state: MakingOrders

        要求を実現するのに必要な注文を算出中です。

        :param _id: 要求ID
        :param requirement: 要求
        :return: 要求の実現に必要な注文
        """
        logger = LoggingAdapter(self._logger,
                                {'class': self.__class__.__name__, 'method': self._making_orders.__name__})
        logger.info(f'@{_id} started')

        # 建玉情報の更新
        remote_positions: NormalizedPositions = await self._get_positions(_id=_id)
        logger.info(f'@{_id} remote positions: {remote_positions}')
        logger.info(f'@{_id} requirement positions: {requirement}')

        if remote_positions:
            to_order: Position = requirement[Symbol.FXBTCJPY] - remote_positions[Symbol.FXBTCJPY]
        else:
            to_order: Position = requirement[Symbol.FXBTCJPY]

        # expire in 30 days
        orders: List[BitflyerOrder] = [BitflyerOrder(
            symbol=to_order.symbol, side=to_order.side, price=int(to_order.price), size=to_order.size,
            child_order_type='LIMIT', minute_to_expire=43200, time_in_force='GTC'
        )]
        logger.info(f'@{_id} built orders: {[str(o) for o in orders]}')
        logger.info(f'@{_id} successfully finished')

        return orders

    async def _ordering(self, _id: str, orders: List[BitflyerOrder]):
        """
        state: Ordering

        新規注文中です。

        :param _id: 要求ID
        :param orders: 要求の実現に必要な注文
        """
        logger = LoggingAdapter(self._logger, {'class': self.__class__.__name__, 'method': self._ordering.__name__})
        logger.info(f'@{_id} started')

        # 新規注文する

        child_order_acceptance_indices: List[str] = list()

        async with self._semaphore:
            logger.info(f'@{_id} exclusive...')

            for order in orders:
                method = 'POST'
                path = '/v1/me/sendchildorder'
                post_body = order.to_http_post_body_sendchildorder()

                code, body = await self._http_client.send_request(
                    _id=_id,
                    response_mapper=BaseResponse,
                    method=method,
                    url=self._req_builder.build_url(path),
                    headers=self._req_builder.build_post_headers(method=method, path=path, post_body=post_body),
                    post_data=post_body,
                )
                logger.info(f'body: {body}')

                if not body:
                    # TODO: HTTP status code 400 時など、詳細を一緒にthrowする
                    raise _UnexpectedResponseException(
                        'Empty response for sendchildorder, it may indicates the order was not accepted'
                    )

                child_order_acceptance_indices.append(body.child_order_acceptance_id)

            logger.info(f'@{_id} inclusive...')

        # 注文が受け付けられたことを確認

        if self._delay:
            logger.info(f'@{_id} wait {self._delay} second')
            await asyncio.sleep(self._delay)

        @retry(wait=wait_fixed(self._http_client.time_wait_retrying),
               before_sleep=before_sleep_log(logger, logging.WARNING))
        async def _ensure_child_order_appeared():
            for child_order_acceptance_id in child_order_acceptance_indices:
                _, active_child_orders = await self._get_child_orders(
                    _id=_id,
                    child_order_state='COMPLETED',
                    child_order_acceptance_id=child_order_acceptance_id
                )
                if active_child_orders.contain(child_order_acceptance_id):
                    logger.info(f'@{_id} confirmed that order was accepted: {child_order_acceptance_id}'
                                f', child_order_state: COMPLETED')
                    return

                if self._delay:
                    logger.info(f'@{_id} wait {self._delay} second')
                    await asyncio.sleep(self._delay)

                _, active_child_orders = await self._get_child_orders(
                    _id=_id,
                    child_order_state='ACTIVE',
                    child_order_acceptance_id=child_order_acceptance_id
                )
                if active_child_orders.contain(child_order_acceptance_id):
                    logger.info(f'@{_id} confirmed that order was accepted: {child_order_acceptance_id}'
                                f', child_order_state: ACTIVE')
                    return

                raise TryAgain(f'@{_id} for child_order_acceptance_id: {child_order_acceptance_id}')

        await _ensure_child_order_appeared()
        logger.info(f'@{_id} successfully finished')

    async def _get_child_orders(self, _id: str, **query) -> Tuple[int, ChildOrders]:
        method = 'GET'
        path = '/v1/me/getchildorders'
        _query = dict(product_code='FX_BTC_JPY', child_order_state='ACTIVE')
        if query:
            _query.update(query)
        headers = self._req_builder.build_get_headers(method=method, path=path, query=_query)

        code: int
        active_child_orders: ChildOrders
        code, active_child_orders = await self._http_client.send_request(
            _id=_id,
            response_mapper=ChildOrders,
            method=method,
            url=self._req_builder.build_url(path),
            headers=headers,
            query=_query,
        )

        return code, active_child_orders

    async def _get_positions(self, _id: str) -> NormalizedPositions:
        """
        合成済みの、建玉情報を返します。
        """
        method = 'GET'
        path = '/v1/me/getpositions'
        query = dict(product_code='FX_BTC_JPY')

        remote_positions: BitflyerPositions
        code, remote_positions = await self._http_client.send_request(
            _id=_id,
            response_mapper=BitflyerPositions,
            method=method,
            url=self._req_builder.build_url(path),
            headers=self._req_builder.build_get_headers(method=method, path=path, query=query),
            query=query,
        )

        normalized: NormalizedPositions = remote_positions.normalize()
        return normalized

    def _state_to(self, state: State):
        self._state = state
        self._logger.info(f'state: {self._state}')


class BitflyerRequestBuilder:

    def __init__(self, logger, api_key, api_secret):
        self._logger = logger
        self.__api_key = api_key
        self.__api_secret = api_secret

        self._base_url = 'https://api.bitflyer.com'

    def build_get_request(self, path: str, params: dict) -> Request:
        query = urlencode(params)
        return Request(
            url=urlunparse(('https', 'api.bitflyer.com', path, '', query, '')),
            headers=self._build_headers(self._generate_timestamp(), 'GET', '{}?{}'.format(path, query), '')
        )

    def build_post_request(self, path, params) -> Request:
        body = dumps(params)
        return Request(
            url=urlunparse(('https', 'api.bitflyer.com', path, '', '', '')),
            headers=self._build_headers(self._generate_timestamp(), 'POST', path, body),
            data=body.encode('utf-8')
        )

    def build_url(self, path) -> str:
        return self._base_url + path

    def build_get_headers(self, method: str, path: str, query: dict) -> Dict[str, str]:
        query = urlencode(query)
        return self._build_headers(self._generate_timestamp(), method, f'{path}?{query}', '')

    def build_post_headers(self, method: str, path: str, post_body: str) -> Dict[str, str]:
        return self._build_headers(self._generate_timestamp(), method, path, post_body)

    def _build_headers(self, timestamp: str, method: str, path: str, body: str) -> Dict[str, str]:
        sign = hmac.new(
            self.__api_secret, ''.join([timestamp, method, path, body]).encode('utf-8'), sha256
        ).hexdigest()
        headers = {
            'ACCESS-KEY': self.__api_key,
            'ACCESS-TIMESTAMP': timestamp,
            'ACCESS-SIGN': sign,
            'Content-Type': 'application/json',
        }
        return headers

    def _generate_timestamp(self):
        return str(datetime.now().timestamp())


def parse_credentials():
    path = os.path.join(config.PROJECT_ROOT, '.credentials')
    parser = ConfigParser()
    if os.path.exists(path):
        with open(path) as fd:
            parser.read_file(fd)
            api_key = parser['bitflyer']['api_key']
            api_secret = parser['bitflyer']['api_secret']
        return api_key, api_secret
    else:
        raise Exception(f'Credentials file {path} not found.')
