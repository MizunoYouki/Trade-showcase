import unittest
from decimal import Decimal

from trade.broker.declarative.bitflyer import BitflyerPositions
from trade.broker.httpclient.response import BaseResponse
from trade.model import Symbol
from trade.side import Side


class BitflyerPositionsTestCase(unittest.TestCase):

    def test_init(self):
        positions = BitflyerPositions([
            {'product_code': 'FX_BTC_JPY', 'side': 'BUY', 'price': 903532.0, 'size': 0.01, 'commission': 0.0,
             'swap_point_accumulate': 0.0, 'require_collateral': 9035.32, 'open_date': '2020-01-13T23:13:01.947',
             'leverage': 1.0, 'pnl': 0.46, 'sfd': 0.0}
        ])
        self.assertEqual(1, len(positions))
        self.assertEqual(Symbol.FXBTCJPY, positions[0].symbol)
        self.assertEqual(Side.BUY, positions[0].side)
        self.assertEqual(Decimal('903532'), positions[0].price)
        self.assertEqual(Decimal('0.01'), positions[0].size)
        self.assertEqual(BaseResponse(dict()), positions.get_fallback())

    def test_fallback(self):
        # noinspection PyTypeChecker
        positions = BitflyerPositions(dict(Message='An error has occurred.'))
        self.assertEqual(BaseResponse(dict(Message='An error has occurred.')), positions.get_fallback())

        # noinspection PyTypeChecker
        positions = BitflyerPositions({
            'foo': 'bar',
            'child': {
                'spam': 124,
                'grandchild': {
                    'ham': 0.12,
                }
            }
        })
        self.assertEqual(
            BaseResponse(dict(foo='bar', child={'spam': 124, 'grandchild': {'ham': 0.12}})),
            positions.get_fallback()
        )


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(BitflyerPositionsTestCase))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='test_suite', verbosity=2, buffer=True)
