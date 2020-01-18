from abc import abstractmethod
from decimal import Decimal
from logging import Logger

from trade.side import Side


# TODO: リファクタリング
class DrawDownBase:

    def __init__(self, logger: Logger):
        self._logger = logger
        self._held_side = Side.NOTHING
        self._held_price = Decimal('NaN')
        self._value = Decimal('0')

    def finish_period(self):
        """
        ドローダウンピリオドを終了します。
        """
        self._held_side = Side.NOTHING
        self._held_price = Decimal('NaN')
        self._value = Decimal('NaN')

    def get_value(self) -> Decimal:
        return self._value

    def get_side(self) -> Side:
        return self._held_side

    def get_initial_price(self) -> Decimal:
        return self._held_price

    @abstractmethod
    def apply(self, price: Decimal):
        raise NotImplementedError()

    def _apply(self, price: Decimal) -> Decimal:
        if self._held_side is Side.NOTHING:
            return Decimal('NaN')

        elif self._held_side is Side.BUY:
            if self._held_price <= price:
                self._logger.info('Draw down recovered')

                return Decimal('0.0')

        elif self._held_side is Side.SELL:
            if price <= self._held_price:
                self._logger.info('Draw down recovered')

                return Decimal('0.0')


# TODO: リファクタリング
class PriceDrawDown(DrawDownBase):
    """
    価格のドローダウン
    """

    def start_period(self, held_side: Side, initial_price: Decimal):
        """
        ドローダウンピリオドを開始します
        :param held_side: 開始時に保持する方向
        :param initial_price: 開始時に保持する価格
        """
        if held_side not in (Side.BUY, Side.SELL):
            raise ValueError(f'Unexpected side: {held_side}')

        self._held_side = held_side
        self._held_price = initial_price

    def apply(self, price: Decimal):
        """
        ドローダウン（価格）を返します
        :param price: 現在の価格
        :return: ドローダウン価格。最大値は、Decimal('0')です。
        """
        if (value := self._apply(price)) is not None:
            self._value = value
        else:
            if self._held_side is Side.BUY:
                price_delta = price - self._held_price
            else:
                price_delta = self._held_price - price

            if Decimal('0') < price_delta:
                self._value = Decimal('0.0')

            self._value = price_delta


# TODO: リファクタリング
class RocDrawDown(DrawDownBase):
    """
    変化率のドローダウン
    """

    def __init__(self, logger: Logger):
        super().__init__(logger)

        self._roc_offset = Decimal('0')

    def start_period(self,
                     held_side: Side,
                     initial_price: Decimal,
                     initial_roc: Decimal):
        """
        ドローダウンピリオドを開始します
        :param held_side: 開始時に保持する方向
        :param initial_price: 開始時に保持する価格
        :param initial_roc: 開始時に保持するROC
        """
        if held_side not in (Side.BUY, Side.SELL):
            raise ValueError(f'Unexpected side: {held_side}')

        self._held_side = held_side
        self._held_price = initial_price
        self._roc_offset = initial_roc

    def apply(self, price: Decimal):
        """
        ドローダウン（変化率）を返します
        :param price: 現在の価格
        :return: ドローダウン変化率。最大値は、Decimal('0')です。
        """
        if (value := self._apply(price)) is not None:
            self._value = value
        else:
            if self._held_side is Side.BUY:
                price_delta = price - self._held_price
            else:
                price_delta = self._held_price - price

            roc = (price_delta / self._held_price) + self._roc_offset

            if Decimal('0') < roc:
                self._value = Decimal('0.0')

            self._value = roc
