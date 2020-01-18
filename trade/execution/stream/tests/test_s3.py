import sys
import unittest
from decimal import Decimal
from io import BytesIO
from lzma import LZMACompressor
from unittest.mock import patch

import numpy as np
from botocore.response import StreamingBody

from trade.execution.model import Execution
from trade.execution.stream.s3 import _ObjectNameV1, _Attribute, S3Stream
from trade.log import get_logger
from trade.model import Symbol, Exchange
from trade.side import Side


class S3StreamTestCase(unittest.IsolatedAsyncioTestCase):
    class _MockStreamingBody(StreamingBody):

        def __init__(self, raw_stream, content_length):
            super().__init__(raw_stream, content_length)

            compressor = LZMACompressor()
            chunk = list()
            for b in raw_stream:
                chunk.append(compressor.compress(b))
            chunk.append(compressor.flush())
            self._bytestream = BytesIO(b''.join(chunk))

        def iter_chunks(self, chunk_size=1024):
            while True:
                chunk = self._bytestream.read(chunk_size)
                if chunk == b'':
                    break
                yield chunk

        def close(self):
            pass

    async def test_aiter_empty(self):
        body = [b'']
        length = len(b''.join(body))
        stream = BytesIO(b''.join(body))

        actual = list()

        with patch('trade.execution.stream.s3.boto3.resource') as mock:
            mock.return_value.Bucket.return_value.Object.return_value.get.return_value = dict(
                Body=self._MockStreamingBody(raw_stream=stream, content_length=length)
            )

            stream = S3Stream(
                logger=get_logger(self.test_aiter_empty.__name__, stream=sys.stdout),
                bucket='chart-mizunoyouki',
                key='FXBTCJPY_bitflyer_executionboard'
                    '/v1/FXBTCJPY_bitflyer_executionboard-v1-2018-12-04T045319.0133528Z.log.xz',
                symbol=Symbol.FXBTCJPY,
            )

            async for execution in stream:
                actual.append(execution)
            self.assertEqual(0, len(actual))

    async def test_aiter(self):
        body = [
            b'{"channel": "lightning_executions_FX_BTC_JPY", '
            b'"message": ['
            b'{"id": 620220851, "side": "SELL", "price": 445893, "size": 0.0853481'
            b', "exec_date": "2018-12-04T09:23:12.5693268Z"'
            b', "buy_child_order_acceptance_id": "JRF20181204-091751-414757"'
            b', "sell_child_order_acceptance_id": "JRF20181204-092312-922802"}'
            b']}\n',
            b'{"channel": "lightning_executions_FX_BTC_JPY", '
            b'"message": ['
            b'{"id": 620220852, "side": "BUY", "price": 445894, "size": 0.1'
            b', "exec_date": "2018-12-04T09:23:12.5693268Z"'
            b', "buy_child_order_acceptance_id": "JRF20181204-091751-414757"'
            b', "sell_child_order_acceptance_id": "JRF20181204-092312-922802"}'
            b']}\n',
            b'{"channel": "lightning_board_FX_BTC_JPY", '
            b'"message": {"mid_price": 445899'
            b', "bids": [{"price": 445893, "size": 9.6506619}, {"price": 445795, "size": 0.2}]'
            b', "asks": [{"price": 446159, "size": 0.25137615}, {"price": 446311, "size": 0.01}]'
            b'}, "appendix": {'
            b'"latest_exec_date": "2018-12-04T09:23:12.5693268Z", "local_date": "2018-12-04T09:23:13.124600Z"'
            b'}}\n',
        ]
        length = len(b''.join(body))
        stream = BytesIO(b''.join(body))

        expected = [
            Execution(symbol=Symbol.FXBTCJPY, _id=620220851,
                      timestamp=np.datetime64('2018-12-04T09:23:12.569326800'), side=Side.SELL,
                      price=Decimal('445893'), size=Decimal('0.0853481'),
                      buy_child_order_acceptance_id='JRF20181204-091751-414757',
                      sell_child_order_acceptance_id='JRF20181204-092312-922802'),
            Execution(symbol=Symbol.FXBTCJPY, _id=620220852,
                      timestamp=np.datetime64('2018-12-04T09:23:12.569326800'), side=Side.BUY,
                      price=Decimal('445894'), size=Decimal('0.1'),
                      buy_child_order_acceptance_id='JRF20181204-091751-414757',
                      sell_child_order_acceptance_id='JRF20181204-092312-922802'),
        ]
        actual = list()

        with patch('trade.execution.stream.s3.boto3.resource') as mock:
            mock.return_value.Bucket.return_value.Object.return_value.get.return_value = dict(
                Body=self._MockStreamingBody(raw_stream=stream, content_length=length)
            )

            stream = S3Stream(
                logger=get_logger(self.test_aiter.__name__, stream=sys.stdout),
                bucket='chart-mizunoyouki',
                key='FXBTCJPY_bitflyer_executionboard'
                    '/v1/FXBTCJPY_bitflyer_executionboard-v1-2018-12-04T045319.0133528Z.log.xz',
                symbol=Symbol.FXBTCJPY,
            )

            async for execution in stream:
                actual.append(execution)
            self.assertEqual(2, len(actual))
            self.assertEqual(expected[0], actual[0])
            self.assertEqual(expected[1], actual[1])


class ObjectNameV1TestCase(unittest.TestCase):

    def test_parse(self):
        self.assertEqual(
            _Attribute(symbol=Symbol.FXBTCJPY, exchange=Exchange.bitFlyer, channel='executionboard',
                       first_datetime=np.datetime64('2018-12-04T04:53:19.013352800', 'ns', utc=True)),
            _ObjectNameV1.parse(
                key='FXBTCJPY_bitflyer_executionboard/v1'
                    '/FXBTCJPY_bitflyer_executionboard-v1-2018-12-04T045319.0133528Z.log.xz'
            )
        )


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(S3StreamTestCase))
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(ObjectNameV1TestCase))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='test_suite', verbosity=2, buffer=True)
