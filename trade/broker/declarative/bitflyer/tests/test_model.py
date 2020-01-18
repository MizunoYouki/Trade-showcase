import unittest
from decimal import Decimal

from trade.broker.declarative.bitflyer.model import BitflyerOrder
from trade.model import Symbol
from trade.side import Side


class BitflyerOrderTestCase(unittest.TestCase):

    def test_init_flooring(self):
        bitflyer_order = BitflyerOrder(
            symbol=Symbol.FXBTCJPY, side=Side.SELL, price=Decimal('993083.0'), size=Decimal('0.01'),
            child_order_type='LIMIT', minute_to_expire=43200, time_in_force='GTC'
        )
        self.assertEqual(Decimal('0.01'), bitflyer_order.size)

        bitflyer_order = BitflyerOrder(
            symbol=Symbol.FXBTCJPY, side=Side.SELL, price=Decimal('993083.0'), size=Decimal('0.009'),
            child_order_type='LIMIT', minute_to_expire=43200, time_in_force='GTC'
        )
        self.assertEqual(Decimal('0'), bitflyer_order.size)


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(BitflyerOrderTestCase))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='test_suite', verbosity=2, buffer=True)
