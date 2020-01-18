import unittest
from decimal import Decimal

from trade.broker.declarative.model import Position, Positions, NormalizedPositions
from trade.model import Symbol
from trade.side import Side


class PositionsTestCase(unittest.TestCase):

    def test_normalize_vwap(self):
        positions = Positions([
            Position(symbol=Symbol.FXBTCJPY, side=Side.BUY, price=Decimal('180.0'), size=Decimal('0.11')),
            Position(symbol=Symbol.FXBTCJPY, side=Side.BUY, price=Decimal('170.0'), size=Decimal('0.0084')),
            Position(symbol=Symbol.FXBTCJPY, side=Side.BUY, price=Decimal('160.0'), size=Decimal('0.0001')),
            Position(symbol=Symbol.BTCJPY, side=Side.SELL, price=Decimal('50.0'), size=Decimal('1.0')),
            Position(symbol=Symbol.ETHBTC, side=Side.BUY, price=Decimal('100.0'), size=Decimal('1.0')),
            Position(symbol=Symbol.ETHBTC, side=Side.BUY, price=Decimal('100.0'), size=Decimal('1.0')),
        ])
        normalized = positions.normalize(method='vwap')

        self.assertEqual(
            NormalizedPositions({
                Symbol.FXBTCJPY: Position(symbol=Symbol.FXBTCJPY, side=Side.BUY,
                                          price=Decimal('179.2742616033755274261603376'),
                                          size=Decimal('0.1185')),
                Symbol.BTCJPY: Position(symbol=Symbol.BTCJPY, side=Side.SELL, price=Decimal('50.0'),
                                        size=Decimal('1.0')),
                Symbol.ETHBTC: Position(symbol=Symbol.ETHBTC, side=Side.BUY, price=Decimal('100.0'),
                                        size=Decimal('2.0'))
            }),
            normalized
        )

    def test_normalize_vwap_side_is_not_unique(self):
        positions = Positions([
            Position(symbol=Symbol.FXBTCJPY, side=Side.BUY, price=Decimal('100.0'), size=Decimal('0.01')),
            Position(symbol=Symbol.FXBTCJPY, side=Side.SELL, price=Decimal('100.0'), size=Decimal('0.1')),
        ])
        self.assertRaises(ValueError, positions.normalize, 'vwap')

    def test_init_empty(self):
        positions = Positions([])
        self.assertEqual(0, len(positions))

    def test_init_error(self):
        self.assertRaises(
            ValueError,
            Positions,
            [
                {'product_code': 'FX_BTC_JPY', 'side': 'BUY', 'price': 903532.0, 'size': 0.01, 'commission': 0.0,
                 'swap_point_accumulate': 0.0, 'require_collateral': 9035.32, 'open_date': '2020-01-13T23:13:01.947',
                 'leverage': 1.0, 'pnl': 0.46, 'sfd': 0.0}
            ]
        )


class PositionTestCase(unittest.TestCase):

    def _p(self, **kwargs) -> Position:
        return Position(symbol=Symbol.FXBTCJPY, **kwargs)

    def test_side_is_nothing(self):
        a = self._p(side=Side.NOTHING, price=Decimal('125'), size=Decimal('8'))
        b = a
        with self.assertRaises(ValueError) as cm:
            a - b
        self.assertEqual(('Could not accept the side other than (Side.BUY, Side.SELL)',), cm.exception.args)

    def test_sub_diff_symbols(self):
        a = Position(symbol=Symbol.FXBTCJPY, side=Side.BUY, price=Decimal('125'), size=Decimal('8'))
        b = Position(symbol=Symbol.BTCJPY, side=Side.BUY, price=Decimal('100'), size=Decimal('1.2'))
        with self.assertRaises(ValueError) as cm:
            a - b
        self.assertEqual(('Symbol is not same (FXBTCJPY != BTCJPY)',), cm.exception.args)

    def test_sub_sameside_sameprice_samesize(self):
        """
        PS(P(BUY, 100, size=1)) - PS(P(BUY, 100, size=1)

        >> PS(P(SELL, 100, size=0))
        """
        a = self._p(side=Side.BUY, price=Decimal('100'), size=Decimal('1'))
        b = self._p(side=Side.BUY, price=Decimal('100'), size=Decimal('1'))
        self.assertEqual(self._p(side=Side.BUY, price=Decimal('100'), size=Decimal('0')), a - b)

    def test_sub_sameside_differentprice_samesize(self):
        """
        PS(P(BUY, 100, size=1)) - PS(P(BUY, 90, size=1)

        >> PS(P(BUY, 100, size=0.1))  # 1 - (90/100 * 1)
        """
        a = self._p(side=Side.BUY, price=Decimal('100'), size=Decimal('1'))
        b = self._p(side=Side.BUY, price=Decimal('90'), size=Decimal('1'))
        self.assertEqual(self._p(side=Side.BUY, price=Decimal('100'), size=Decimal('0.1')), a - b)

        """
        PS(P(BUY, 100, size=1)) - PS(P(BUY, 110, size=1)

        >> PS(P(SELL, 100, size=0.1))  # 1 - (110/100 * 1)
        """
        a = self._p(side=Side.BUY, price=Decimal('100'), size=Decimal('1'))
        b = self._p(side=Side.BUY, price=Decimal('110'), size=Decimal('1'))
        self.assertEqual(self._p(side=Side.SELL, price=Decimal('100'), size=Decimal('0.1')), a - b)

    def test_sub_sameside_sameprice_diffrentsize(self):
        """
        PS(P(BUY, 100, size=1)) - PS(P(BUY, 100, size=1.3)

        >> PS(P(SELL, 100, size=0.3))  # 1 - (100/100 * 1.3)
        """
        a = self._p(side=Side.BUY, price=Decimal('100'), size=Decimal('1'))
        b = self._p(side=Side.BUY, price=Decimal('100'), size=Decimal('1.3'))
        self.assertEqual(self._p(side=Side.SELL, price=Decimal('100'), size=Decimal('0.3')), a - b)

        """
        PS(P(BUY, 100, size=1)) - PS(P(BUY, 100, size=0.6)

        >> PS(P(BUY, 100, size=0.4))  # 1 - (100/100 * 0.6)
        """
        a = self._p(side=Side.BUY, price=Decimal('100'), size=Decimal('1'))
        b = self._p(side=Side.BUY, price=Decimal('100'), size=Decimal('0.6'))
        self.assertEqual(self._p(side=Side.BUY, price=Decimal('100'), size=Decimal('0.4')), a - b)

    def test_sub_sameside_differentprice_differentsize(self):
        """
        PS(P(BUY, 100, size=1)) - PS(P(BUY, 90, size=1.3)

        >> PS(P(SELL, 100, size=0.17))  # 1 - (90/100 * 1.3)
        """
        a = self._p(side=Side.BUY, price=Decimal('100'), size=Decimal('1'))
        b = self._p(side=Side.BUY, price=Decimal('90'), size=Decimal('1.3'))
        self.assertEqual(self._p(side=Side.SELL, price=Decimal('100'), size=Decimal('0.17')), a - b)

        """
        PS(P(BUY, 100, size=1)) - PS(P(BUY, 90, size=0.6)
        
        >> PS(P(BUY, 100, size=0.46))  # 1 - (90/100 * 0.6)
        """
        a = self._p(side=Side.BUY, price=Decimal('100'), size=Decimal('1'))
        b = self._p(side=Side.BUY, price=Decimal('90'), size=Decimal('0.6'))
        self.assertEqual(self._p(side=Side.BUY, price=Decimal('100'), size=Decimal('0.46')), a - b)

        """
        PS(P(BUY, 100, size=1)) - PS(P(BUY, 110, size=1.3)

        >> PS(P(SELL, 100, size=0.43))  # 1 - (110/100 * 1.3)
        """
        a = self._p(side=Side.BUY, price=Decimal('100'), size=Decimal('1'))
        b = self._p(side=Side.BUY, price=Decimal('110'), size=Decimal('1.3'))
        self.assertEqual(self._p(side=Side.SELL, price=Decimal('100'), size=Decimal('0.43')), a - b)

        """
        PS(P(BUY, 100, size=1)) - PS(P(BUY, 110, size=0.6)

        >> PS(P(BUY, 100, size=0.37))  # 1 - (110/100 * 0.6)
        """
        a = self._p(side=Side.BUY, price=Decimal('100'), size=Decimal('1'))
        b = self._p(side=Side.BUY, price=Decimal('110'), size=Decimal('0.6'))
        self.assertEqual(self._p(side=Side.BUY, price=Decimal('100'), size=Decimal('0.34')), a - b)

    def test_sub_differentside_sameprice_samesize(self):
        """
        PS(P(SELL, 100, size=1)) - PS(P(BUY, 100, size=1)

        >> PS(P(SELL, 100, size=2))
        """
        a = self._p(side=Side.SELL, price=Decimal('100'), size=Decimal('1'))
        b = self._p(side=Side.BUY, price=Decimal('100'), size=Decimal('1'))
        self.assertEqual(self._p(side=Side.SELL, price=Decimal('100'), size=Decimal('2')), a - b)

    def test_sub_differentside_differentprice_samesize(self):
        """
        PS(P(SELL, 100, size=1)) - PS(P(BUY, 90, size=1)

        >> PS(P(SELL, 100, size=1.9))  # 1 + (90/100 * 1)
        """
        a = self._p(side=Side.SELL, price=Decimal('100'), size=Decimal('1'))
        b = self._p(side=Side.BUY, price=Decimal('90'), size=Decimal('1'))
        self.assertEqual(self._p(side=Side.SELL, price=Decimal('100'), size=Decimal('1.9')), a - b)

        """
        PS(P(SELL, 100, size=1)) - PS(P(BUY, 110, size=1)

        >> PS(P(SELL, 100, size=2.1))  # 1 + (110/100 * 1)
        """
        a = self._p(side=Side.SELL, price=Decimal('100'), size=Decimal('1'))
        b = self._p(side=Side.BUY, price=Decimal('110'), size=Decimal('1'))
        self.assertEqual(self._p(side=Side.SELL, price=Decimal('100'), size=Decimal('2.1')), a - b)

    def test_sub_differentside_sameprice_differentsize(self):
        """
        PS(P(SELL, 100, size=1)) - PS(P(BUY, 100, size=1.3)

        >> PS(P(SELL, 100, size=2.3))  # 1 + (100/100 * 1.3)
        """
        a = self._p(side=Side.SELL, price=Decimal('100'), size=Decimal('1'))
        b = self._p(side=Side.BUY, price=Decimal('100'), size=Decimal('1.3'))
        self.assertEqual(self._p(side=Side.SELL, price=Decimal('100'), size=Decimal('2.3')), a - b)

        """
        PS(P(SELL, 100, size=1)) - PS(P(BUY, 100, size=0.6)

        >> PS(P(SELL, 100, size=1.6))  # 1 + (100/100 * 0.6)
        """
        a = self._p(side=Side.SELL, price=Decimal('100'), size=Decimal('1'))
        b = self._p(side=Side.BUY, price=Decimal('100'), size=Decimal('0.6'))
        self.assertEqual(self._p(side=Side.SELL, price=Decimal('100'), size=Decimal('1.6')), a - b)

    def test_sub_differentside_differentprice_differentsize(self):
        """
        PS(P(SELL, 100, size=1)) - PS(P(BUY, 90, size=1.3)

        >> PS(P(SELL, 100, size=2.17))  # 1 + (90/100 * 1.3)
        """
        a = self._p(side=Side.SELL, price=Decimal('100'), size=Decimal('1'))
        b = self._p(side=Side.BUY, price=Decimal('90'), size=Decimal('1.3'))
        self.assertEqual(self._p(side=Side.SELL, price=Decimal('100'), size=Decimal('2.17')), a - b)

        """
        PS(P(SELL, 100, size=1)) - PS(P(BUY, 90, size=0.6)

        >> PS(P(SELL, 100, size=1.54))  # 1 + (90/100 * 0.6)
        """
        a = self._p(side=Side.SELL, price=Decimal('100'), size=Decimal('1'))
        b = self._p(side=Side.BUY, price=Decimal('90'), size=Decimal('0.6'))
        self.assertEqual(self._p(side=Side.SELL, price=Decimal('100'), size=Decimal('1.54')), a - b)

        """
        PS(P(SELL, 100, size=1)) - PS(P(BUY, 110, size=1.3)

        >> PS(P(SELL, 100, size=2.43))  # 1 + (110/100 * 1.3)
        """
        a = self._p(side=Side.SELL, price=Decimal('100'), size=Decimal('1'))
        b = self._p(side=Side.BUY, price=Decimal('110'), size=Decimal('1.3'))
        self.assertEqual(self._p(side=Side.SELL, price=Decimal('100'), size=Decimal('2.43')), a - b)

        """
        PS(P(SELL, 100, size=1)) - PS(P(BUY, 110, size=0.6)

        >> PS(P(SELL, 100, size=1.66))  # 1 + (110/100 * 0.6)
        """
        a = self._p(side=Side.SELL, price=Decimal('100'), size=Decimal('1'))
        b = self._p(side=Side.BUY, price=Decimal('110'), size=Decimal('0.6'))
        self.assertEqual(self._p(side=Side.SELL, price=Decimal('100'), size=Decimal('1.66')), a - b)

    def test_sub_sameside_size_accuracy(self):
        requirement: Position = Position(
            symbol=Symbol.FXBTCJPY, side=Side.SELL, price=Decimal('993083.0'), size=Decimal('0.01')
        )
        remote_positions: Position = Position(
            symbol=Symbol.FXBTCJPY, side=Side.SELL, price=Decimal('992600.0'), size=Decimal('0.009')
        )
        self.assertEqual(
            Position(symbol=Symbol.FXBTCJPY, side=Side.SELL, price=Decimal('993083.0'),
                     size=Decimal('0.001004377277629362299022337508')),
            requirement - remote_positions
        )

    def test_sub_differentside_size_accuracy(self):
        requirement: Position = Position(
            symbol=Symbol.FXBTCJPY, side=Side.BUY, price=Decimal('993083.0'), size=Decimal('0.01')
        )
        remote_positions: Position = Position(
            symbol=Symbol.FXBTCJPY, side=Side.SELL, price=Decimal('992600.0'), size=Decimal('0.01')
        )
        self.assertEqual(
            Position(symbol=Symbol.FXBTCJPY, side=Side.BUY, price=Decimal('993083.0'),
                     size=Decimal('0.01999513635818959744553073610')),
            requirement - remote_positions
        )


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(PositionTestCase))
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(PositionsTestCase))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='test_suite', verbosity=2, buffer=True)
