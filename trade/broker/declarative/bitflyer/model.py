from decimal import Decimal, ROUND_DOWN
from json import dumps
from typing import Mapping, Any, Optional

from trade.broker.declarative.model import Order
from trade.broker.httpclient.response import BaseResponse
from trade.model import Symbol
from trade.side import Side


def _encode_product_code(product_code: str) -> Symbol:
    if product_code == 'FX_BTC_JPY':
        return Symbol.FXBTCJPY

    raise ValueError(f'Acceptable product_code is FX_BTC_JPY only at this time (passed: {product_code})')


class ChildOrder(BaseResponse):
    id: int
    child_order_id: str
    child_order_acceptance_id: str
    product_code: str
    side: Side
    child_order_type: str
    price: Decimal
    size: Decimal
    child_order_state: str
    executed_size: Decimal
    symbol: Symbol

    def __init__(self, raw: Mapping[str, Any]):
        super().__init__(raw)

        self.symbol = _encode_product_code(self.product_code)

    def __str__(self):
        return f'CO({self._str_args()})'

    def to_http_post_body_cancelchildorder(self) -> str:
        return dumps(dict(product_code=self.product_code, child_order_id=self.child_order_id))


class BitflyerOrder(Order):
    """
    Bitflyer注文モデル

    Orderとの違いは以下です。
    - product_code属性が追加されている
    - priceがint型である
    - sizeの小数点以下桁数が2である
    """
    symbol: Symbol
    product_code: str
    side: Side
    price: int
    size: Decimal
    child_order_type: str
    minute_to_expire: int
    time_in_force: str

    def __init__(self, symbol: Symbol, side: Side, price: int, size: Decimal,
                 child_order_type: str, minute_to_expire: int, time_in_force: str):
        if symbol is Symbol.FXBTCJPY:
            self.product_code = 'FX_BTC_JPY'
        else:
            raise ValueError(f'Acceptable symbol is {Symbol.FXBTCJPY!r} only at this time (passed: {symbol!r})')

        super().__init__(
            symbol=symbol,
            side=side,
            price=Decimal(str(price)),
            size=Decimal(str(size)),
            child_order_type=child_order_type,
            minute_to_expire=minute_to_expire,
            time_in_force=time_in_force
        )

        self.price = int(price)
        self.size = size.quantize(Decimal('0.01'), rounding=ROUND_DOWN)
        self.amount = self.size * self.price

    def to_http_post_body_sendchildorder(self) -> str:
        return dumps(dict(
            product_code=self.product_code,
            child_order_type=self.child_order_type,
            side=self.side.name.upper(),
            size=float(self.size),
            price=self.price,
            minute_to_expire=self.minute_to_expire,
            time_in_force=self.time_in_force,
        ))

    @staticmethod
    def minimum_size() -> Optional[Decimal]:
        return Decimal('0.01')
