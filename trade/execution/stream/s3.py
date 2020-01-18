import json
import lzma
import os
from dataclasses import dataclass
from datetime import datetime
from logging import Logger
from typing import AsyncIterable, AsyncIterator, Iterator, Any, Dict, Optional, List

import boto3
import numpy as np
import pandas as pd
from botocore.response import StreamingBody

from trade.execution.model import Execution
from trade.model import Exchange, Symbol, normalize_exchange_name


class S3Stream(AsyncIterable[Execution]):
    """
    AWS S3オブジェクト を源とする、Executionストリーム
    """

    def __init__(self, logger: Logger, bucket: str, key: str, symbol: Symbol):
        self._logger = logger
        self._symbol = symbol

        self._logger.info(f'started to get: bucket: {bucket}, key: {key}')
        self._s3_streaming: StreamingBody = boto3.resource('s3').Bucket(bucket).Object(key).get()['Body']

    def __del__(self):
        self._s3_streaming.close()

    async def __aiter__(self) -> AsyncIterator[Execution]:
        buf: List[bytes] = list()
        for chunk in self._s3_streaming.iter_chunks(1024 * 1000 * 1000):
            buf.append(chunk)

        binary = lzma.decompress(b''.join(buf))

        for line in [line.decode('utf-8') for line in binary.splitlines()]:
            if 'lightning_executions_' not in line:  # TODO: bitFlyerの他はどうする？
                continue
            _json = json.loads(line)
            for message in _json['message']:
                execution = Execution.encode_bitflyer_response(symbol=self._symbol, dictobj=message)
                yield execution


class _ObjectNameV1:
    """
    Object name parser/unparser for S3 object of version 1.

    Object name is like to ...
    `FXBTCJPY_bitflyer_executionboard/v1/FXBTCJPY_bitflyer_executionboard-v1-2018-12-04T045319.0133528Z.log.xz`
    """

    @staticmethod
    def parse(key: str) -> '_Attribute':
        basename = os.path.basename(key)
        e: List[str] = basename.split('_')
        symbol = Symbol(e[0])
        exchange = Exchange(normalize_exchange_name(e[1]))
        channel, _, dt = e[2].split('-', maxsplit=2)
        dt = np.datetime64(pd.to_datetime(dt.replace('.log.xz', ''), utc=True).asm8, utc=True)
        return _Attribute(
            symbol=symbol, exchange=exchange, channel=channel, first_datetime=dt
        )


@dataclass
class _Attribute:
    symbol: Symbol
    exchange: Exchange
    channel: str
    first_datetime: np.datetime64


def build_s3_key_prefix(logger: Logger,
                        symbol: Symbol,
                        exchange: Exchange,
                        channel: str,
                        version: int,
                        year: Optional[int] = None,
                        month: Optional[int] = None,
                        day: Optional[int] = None) -> str:
    """
    S3 Keyプレフィックスを組み立てて返します。

    :param logger: ロガーオブジェクト
    :param symbol: シンボル
    :param exchange: エクスチェンジ
    :param channel: チャネル
    :param version: バージョン
    :param year: 指定された場合、この値がS3キーの絞り込みのために使われます。S3 List objectの回数削減に役立ちます。
    :param month: see `year`
    :param day: see `year`
    :return: S3 Keyプレフィックス
    """
    common = f'{symbol.value}_{exchange.value.lower()}_{channel}'
    e = list()

    e.append(f'{common}/v{version}/{common}-v{version}-')

    if year:
        e.append(str(year))
        if month:
            e.append(f'-{month:0>2}')
            if day:
                e.append(f'-{day:0>2}')

    return ''.join(e)


def list_s3_keys(logger: Logger,
                 bucket: str,
                 s3_key_prefix: Optional[str],
                 datetime_from: Optional[np.datetime64]) -> Iterator[str]:
    """
    S3 Keyのイテレータを返します。

    :param logger: ロガーオブジェクト
    :param bucket: 対象S3バケット
    :param s3_key_prefix: 指定された場合、この値がS3キーの絞り込みのために使われます。S3 List objectの回数削減に
    役立ちます。
    :param datetime_from: 指定された場合、この値をExecutionタイムスタンプとして含むS3オブジェクトのキー、およびより新しい
    Executionが保存されているS3オブジェクトのキーだけを返します。
    :return: S3 Keyのイテレータ。Keyの昇順にソートされています。
    """
    logger.info(f's3 key prefix: {s3_key_prefix}')

    s3 = boto3.client('s3')
    params = {'Bucket': bucket, 'Prefix': s3_key_prefix, 'MaxKeys': 1000}
    continuation_token: Optional[str] = None
    keys: List[str] = list()

    while True:
        if continuation_token:
            params['ContinuationToken'] = continuation_token

        response: Dict[Any, Any] = s3.list_objects_v2(**params)

        if 'Contents' not in response:
            return

        for content in response['Contents']:
            key: str = content['Key']

            if key.endswith('/'):
                continue

            keys.append(key)

        if 'IsTruncated' in response:
            if not response['IsTruncated']:
                break

        if 'NextContinuationToken' in response:
            continuation_token = response['NextContinuationToken']
        else:
            continuation_token = None

    keys.sort()
    prev = None
    firstly = True

    if not np.isnat(datetime_from):
        for key in keys:
            if not prev:
                prev = key
                continue

            first_datetime: np.datetime64 = _ObjectNameV1.parse(key).first_datetime
            if datetime_from < first_datetime:
                if firstly:
                    firstly = False
                    yield prev
                yield key

            prev = key

    else:
        yield from keys


if __name__ == '__main__':
    import sys
    import asyncio
    from trade.log import get_logger


    async def main(stream: S3Stream):
        async for execution in stream:
            print(execution)


    _logger = get_logger(__name__, stream=sys.stdout, _format='%(asctime)s:%(levelname)s:%(message)s')
    for _key in list_s3_keys(
            logger=_logger,
            bucket='chart-mizunoyouki',
            s3_key_prefix=build_s3_key_prefix(
                _logger, symbol=Symbol.FXBTCJPY, exchange=Exchange.bitFlyer, channel='executionboard', version=1,
                year=2020, month=1
            ),
            datetime_from=np.datetime64(datetime(2020, 1, 9)),
    ):
        _logger.info(f's3 key: {_key}')
        _stream = S3Stream(
            logger=get_logger(name=__name__, stream=sys.stdout),
            bucket='chart-mizunoyouki',
            key=_key,
            symbol=Symbol.FXBTCJPY
        )
        asyncio.run(main(_stream))
