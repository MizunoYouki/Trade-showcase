import unittest
from decimal import Decimal

from trade.log import get_logger
from trade.side import Side
from trade.strategy.internal.risk import PriceDrawDown, RocDrawDown


class PriceDrawDownTestCase(unittest.TestCase):

    def test_apply(self):
        logger = get_logger(self.test_apply.__name__)

        dd = PriceDrawDown(logger=logger)

        dd.apply(Decimal('100.0'))
        self.assertTrue(dd.get_value().is_nan())

        dd.start_period(held_side=Side.BUY, initial_price=Decimal('100.0'))

        dd.apply(price=Decimal('100.0'))
        self.assertEqual(Decimal('0.0'), dd.get_value())
        dd.apply(price=Decimal('100.1'))
        self.assertEqual(Decimal('0.0'), dd.get_value())
        dd.apply(price=Decimal('99'))
        self.assertEqual(Decimal('-1.0'), dd.get_value())
        dd.apply(price=Decimal('90'))
        self.assertEqual(Decimal('-10.0'), dd.get_value())

        dd.finish_period()

        dd.apply(Decimal('1.0'))
        self.assertTrue(dd.get_value().is_nan())
        dd.apply(Decimal('2.0'))
        self.assertTrue(dd.get_value().is_nan())

        dd.start_period(held_side=Side.BUY, initial_price=Decimal('90'))

        dd.apply(price=Decimal('90.0'))
        self.assertEqual(Decimal('0.0'), dd.get_value())
        dd.apply(price=Decimal('81.0'))
        self.assertEqual(Decimal('-9.0'), dd.get_value())
        dd.apply(price=Decimal('82.0'))
        self.assertEqual(Decimal('-8.0'), dd.get_value())

        dd.start_period(held_side=Side.SELL, initial_price=Decimal('100'))

        dd.apply(price=Decimal('99.9'))
        self.assertEqual(Decimal('0.0'), dd.get_value())
        dd.apply(price=Decimal('100.1'))
        self.assertEqual(Decimal('-0.1'), dd.get_value())

        dd.finish_period()

        dd.apply(Decimal('3'))
        self.assertTrue(dd.get_value().is_nan())
        dd.apply(Decimal('4'))
        self.assertTrue(dd.get_value().is_nan())


class PercentageDrawDownTestCase(unittest.TestCase):

    def test_apply(self):
        logger = get_logger(self.test_apply.__name__)

        dd = RocDrawDown(logger=logger)

        dd.apply(Decimal('100.0'))
        self.assertTrue(dd.get_value().is_nan())

        dd.start_period(held_side=Side.BUY, initial_price=Decimal('1000.0'), initial_roc=Decimal('0'))

        dd.apply(price=Decimal('1000.0'))
        self.assertEqual(Decimal('0.0'), dd.get_value())
        dd.apply(price=Decimal('1001'))
        self.assertEqual(Decimal('0.0'), dd.get_value())
        dd.apply(price=Decimal('999'))
        self.assertEqual(Decimal('-0.001'), dd.get_value())

        dd.finish_period()

        dd.apply(Decimal('1.0'))
        self.assertTrue(dd.get_value().is_nan())
        dd.apply(Decimal('2.0'))
        self.assertTrue(dd.get_value().is_nan())

        dd.start_period(held_side=Side.BUY, initial_price=Decimal('10000.0'), initial_roc=Decimal('0'))

        dd.apply(price=Decimal('10010.0'))
        self.assertEqual(Decimal('0.0'), dd.get_value())
        dd.apply(price=Decimal('9999'))
        self.assertEqual(Decimal('-0.0001'), dd.get_value())
        dd.apply(price=Decimal('9000'))
        self.assertEqual(Decimal('-0.1'), dd.get_value())

        dd.finish_period()

        dd.apply(Decimal('-1.0'))
        self.assertTrue(dd.get_value().is_nan())
        dd.apply(Decimal('-2.0'))
        self.assertTrue(dd.get_value().is_nan())

        dd.start_period(held_side=Side.SELL, initial_price=Decimal('10000.0'), initial_roc=Decimal('0'))

        dd.apply(price=Decimal('9999.0'))
        self.assertEqual(Decimal('0.0'), dd.get_value())
        dd.apply(price=Decimal('10100'))
        self.assertEqual(Decimal('-0.01'), dd.get_value())

        dd.start_period(held_side=Side.BUY, initial_price=Decimal('100.0'), initial_roc=Decimal('0.01'))

        dd.apply(price=Decimal('100.0'))
        self.assertEqual(Decimal('0.0'), dd.get_value())
        dd.apply(price=Decimal('101'))
        self.assertEqual(Decimal('0.0'), dd.get_value())
        dd.apply(price=Decimal('99'))
        self.assertEqual(Decimal('0.0'), dd.get_value())
        dd.apply(price=Decimal('98'))
        self.assertEqual(Decimal('-0.01'), dd.get_value())


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(PriceDrawDownTestCase))
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(PercentageDrawDownTestCase))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='test_suite', verbosity=2, buffer=True)
