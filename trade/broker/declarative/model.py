from dataclasses import dataclass
from decimal import Decimal, getcontext, FloatOperation
from typing import List, Optional, Dict

from trade.model import Symbol
from trade.side import Side, counter_side

getcontext().traps[FloatOperation] = True


@dataclass
class Order:
    """
    注文
    """
    symbol: Symbol
    side: Side
    price: Decimal
    size: Decimal
    child_order_type: str
    minute_to_expire: int
    time_in_force: str

    def __repr__(self):
        return f"{self.__class__.__name__}(symbol={self.symbol!r}, side={self.side!r}" \
               f", price={self.price!r}, size={self.size!r}, child_order_type='{self.child_order_type}'," \
               f" minute_to_expire={self.minute_to_expire}, time_in_force='{self.time_in_force})"

    def __str__(self):
        return f"{self.__class__.__name__}(symbol={self.symbol!s}, side={self.side!s}" \
               f", price={self.price!s}, size={self.size!s}, child_order_type={self.child_order_type}," \
               f" minute_to_expire={self.minute_to_expire}, time_in_force={self.time_in_force})"

    @staticmethod
    def minimum_size() -> Optional[Decimal]:
        return None


@dataclass
class Position:
    """
    建玉
    """
    symbol: Symbol
    side: Side
    price: Decimal
    size: Decimal

    def __str__(self):
        return f'P(symbol={self.symbol.value}, side={self.side.value}, price={self.price!s}, size={self.size!s})'

    def __sub__(self, other: 'Position') -> 'Position':
        if self.side not in (Side.BUY, Side.SELL) or other.side not in (Side.BUY, Side.SELL):
            raise ValueError(f'Could not accept the side other than ({Side.BUY!s}, {Side.SELL!s})')

        if self.symbol is not other.symbol:
            raise ValueError(f'Symbol is not same ({self.symbol.value} != {other.symbol.value})')

        # Same side
        if self.side is other.side:
            size_insufficient = self.size - ((other.price / self.price) * other.size)
            if 0 <= size_insufficient:
                return Position(symbol=self.symbol, side=self.side, price=self.price, size=size_insufficient)
            else:
                return Position(symbol=self.symbol, side=counter_side(self.side), price=self.price,
                                size=-size_insufficient)

        # Different side
        other_volume = (other.price / self.price) * other.size
        return Position(symbol=self.symbol, side=self.side, price=self.price, size=self.size + other_volume)


class Positions(List[Position]):
    """
    複数の建玉
    """

    @dataclass(frozen=True)
    class _Product:
        symbol: Symbol
        side: Side

    class _Total:
        def __init__(self, size: Decimal, price: Decimal):
            self.size = size
            self.amount = size * price

        def add(self, size: Decimal, price: Decimal):
            self.size += size
            self.amount += (size * price)

    def __init__(self, seq: List[Position]):
        if seq:
            for position in seq:
                if not isinstance(position, Position):
                    raise ValueError(f'Should be inited by `Position` type, got: {position}')

        super().__init__(seq)

    def __repr__(self):
        return f'{self.__class__.__name__}({self}))'

    def __str__(self):
        return f'PS({[str(p) for p in self]}))'

    def normalize(self, method='vwap') -> 'NormalizedPositions':
        """
        同一シンボルを持つ、複数のPositionを含む場合に、正規化して返します。
        :param method: 正規化の方法。"vwap"は加重平均を用います。現在は"vwap"のみ受け付けます。
        :return: 正規化された、複数の建玉
        """
        if method != 'vwap':
            raise ValueError(f'Unexpected method: {method}')

        totals: Dict[Positions._Product, Positions._Total] = dict()
        for p in self:
            product = self._Product(p.symbol, p.side)

            if product in totals:
                totals[product].add(size=p.size, price=p.price)
            else:
                counter_side_product = self._Product(product.symbol, counter_side(product.side))
                if counter_side_product in totals:
                    raise ValueError('Could not normalize the position (`side` is not unique)')

                totals[product] = self._Total(size=p.size, price=p.price)

        normalized = NormalizedPositions()
        for product, total in totals.items():
            normalized[product.symbol] = Position(
                symbol=product.symbol, side=product.side, price=total.amount / total.size, size=total.size
            )
        return normalized


class NormalizedPositions(Dict[Symbol, Position]):
    """
    正規化済みの、複数の建玉
    """

    def __str__(self):
        elements = [f'{k!s}: {v!s}' for k, v in self.items()]
        return f"<NPS{''.join(['{', ', '.join(elements), '}'])}>"
