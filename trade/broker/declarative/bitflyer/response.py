from decimal import Decimal
from typing import List, Mapping, Any, Dict

from trade.broker.declarative.bitflyer import ChildOrder
from trade.broker.declarative.bitflyer.model import _encode_product_code
from trade.broker.declarative.model import Positions, Position
from trade.broker.httpclient.response import FallbackMixin, BaseResponse
from trade.side import Side


class ChildOrders(List[ChildOrder], FallbackMixin):

    def __init__(self, raw: List[Mapping[str, Any]]):
        try:
            super().__init__([ChildOrder(o) for o in raw])

        except TypeError:
            # noinspection PyTypeChecker
            self._fallback = BaseResponse(raw)

    def contain(self, child_order_acceptance_id: str):
        return any([child_order_acceptance_id == co.child_order_acceptance_id for co in self])


class BitflyerPositions(Positions, FallbackMixin):

    def __init__(self, seq: List[Dict[str, Any]]):
        try:
            super().__init__([
                Position(symbol=_encode_product_code(raw['product_code']), side=Side(raw['side']),
                         price=Decimal(str(raw['price'])), size=Decimal(str(raw['size'])))
                for raw in seq
            ])
            self._fallback = BaseResponse(dict())

        except TypeError:
            # noinspection PyTypeChecker
            self._fallback = BaseResponse(seq)
