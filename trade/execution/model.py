from dataclasses import dataclass
from decimal import Decimal
from typing import Union, Optional, Mapping

import numpy as np

from trade.model import Symbol
from trade.side import Side


@dataclass
class SwitchedToRealtime:
    """
    Executionストリームがリアルタイムになったことをあらわす
    """
    symbol: Symbol
    timestamp: np.datetime64

    def __init__(self, symbol: Symbol, timestamp: np.datetime64):
        self.symbol = symbol
        self.timestamp = timestamp

    def __repr__(self):
        return f'{self.__class__.__name__}' \
               f'(symbol={Symbol.__name__}.{self.symbol.value}, ' \
               f'timestamp=numpy.datetime64("{self.timestamp}", "ns", utc=True))'


def encode_bitflyer_channel(channel: str) -> Symbol:
    if channel == 'lightning_executions_FX_BTC_JPY':
        return Symbol.FXBTCJPY
    elif channel == 'lightning_executions_BTC_JPY':
        return Symbol.BTCJPY
    else:
        raise Exception(f'Unexpected channel: {channel}')


class Execution:

    def __init__(self,
                 symbol: Symbol,
                 _id: Union[int, SwitchedToRealtime, None],
                 timestamp: np.datetime64,
                 side: Optional[Side],
                 price: Decimal,
                 size: Decimal,
                 buy_child_order_acceptance_id: str,
                 sell_child_order_acceptance_id: str,
                 timeunit_if_ohlc_from: Optional[np.timedelta64] = None,
                 synchronized_execution_price_deviation: Optional[Decimal] = None,
                 synchronized_execution_time_delta: Optional[np.timedelta64] = None,
                 synchronized_execution: Optional['SynchronizedExecution'] = None,
                 **attrs):
        self.symbol = symbol
        self._id = _id
        self.timestamp = timestamp
        self.side = side
        self.price = price
        self.size = size
        self.buy_child_order_acceptance_id = buy_child_order_acceptance_id
        self.sell_child_order_acceptance_id = sell_child_order_acceptance_id

        # TODO: もはや不要では？
        self.timeunit_if_ohlc_from = timeunit_if_ohlc_from

        self.synchronized_execution_price_deviation = synchronized_execution_price_deviation
        self.synchronized_execution_time_delta = synchronized_execution_time_delta
        self.synchronized_execution = synchronized_execution
        self.attrs = attrs

    def __str__(self):
        return f'{self.__class__.__name__}(symbol={self.symbol.value}' \
               f', _id={self._id}' \
               f', timestamp={self.timestamp!s}, side={self.side!s}, price={self.price!s}, size={self.size!s}' \
               f', buy_child_order_acceptance_id={self.buy_child_order_acceptance_id!s}' \
               f', sell_child_order_acceptance_id={self.sell_child_order_acceptance_id!s}' \
               f', timeunit_if_ohlc_from={self.timeunit_if_ohlc_from!s}' \
               f', synchronized_execution_price_deviation={self.synchronized_execution_price_deviation!s}' \
               f', synchronized_execution_time_delta={self.synchronized_execution_time_delta!s}' \
               f', synchronized_execution={self.synchronized_execution!s}' \
               f', attrs={self.attrs!s})'

    def __repr__(self):
        return f'{self.__class__.__name__}(symbol={Symbol.__name__}.{self.symbol.value!s}' \
               f', _id={self._id}' \
               f', timestamp={self.timestamp!r}, side={self.side!s}, price={self.price!r}, size={self.size!r}' \
               f', buy_child_order_acceptance_id={self.buy_child_order_acceptance_id!r}' \
               f', sell_child_order_acceptance_id={self.sell_child_order_acceptance_id!r}' \
               f', timeunit_if_ohlc_from={self.timeunit_if_ohlc_from!r}' \
               f', synchronized_execution_price_deviation={self.synchronized_execution_price_deviation!r}' \
               f', synchronized_execution_time_delta={self.synchronized_execution_time_delta!r}' \
               f', synchronized_execution={self.synchronized_execution!r}' \
               f', attrs={self.attrs!r})'

    def __eq__(self, other: 'Execution'):
        return self.symbol == other.symbol and \
               self._id == other._id and \
               self.timestamp == other.timestamp and \
               self.side == other.side and \
               self.price == other.price and \
               self.size == other.size and \
               self.buy_child_order_acceptance_id == other.buy_child_order_acceptance_id and \
               self.sell_child_order_acceptance_id == other.sell_child_order_acceptance_id and \
               self.timeunit_if_ohlc_from == other.timeunit_if_ohlc_from and \
               self.synchronized_execution_price_deviation == other.synchronized_execution_price_deviation and \
               self.synchronized_execution_time_delta == other.synchronized_execution_time_delta and \
               self.synchronized_execution == other.synchronized_execution and \
               self.attrs == other.attrs

    @staticmethod
    def encode_bitflyer_response(symbol: Symbol,
                                 dictobj: Mapping[str, Union[str, int]]) -> 'Execution':
        return Execution(
            symbol=symbol,
            _id=dictobj['id'],
            timestamp=np.datetime64(dictobj['exec_date'].rstrip('Z'), 'ns', utc=True),
            side=dictobj['side'] and Side(dictobj['side']) or Side.NOTHING,
            price=Decimal(str(dictobj['price'])),
            size=Decimal(str(dictobj['size'])),
            buy_child_order_acceptance_id=dictobj['buy_child_order_acceptance_id'],
            sell_child_order_acceptance_id=dictobj['sell_child_order_acceptance_id'],
        )

    @staticmethod
    def encode_bitflyer_response_raw(symbol: Symbol,
                                     dictobj: Mapping[str, Union[str, int]]) -> 'Execution':
        return Execution(
            symbol=symbol,
            _id=dictobj['id'],
            timestamp=np.datetime64(dictobj['exec_date'].rstrip('Z'), 'ns', utc=True),
            side=Side(dictobj['side']),
            price=Decimal(str(dictobj['price'])),
            size=Decimal(str(dictobj['size'])),
            buy_child_order_acceptance_id=dictobj['buy_child_order_acceptance_id'],
            sell_child_order_acceptance_id=dictobj['sell_child_order_acceptance_id'],
            raw_response=dictobj['raw_response'],
        )

    @staticmethod
    def wrap(execution: 'Execution',
             timeunit_if_ohlc_from: Optional[np.timedelta64] = None,
             synchronized_execution: Optional['SynchronizedExecution'] = None) -> 'Execution':
        return Execution(
            symbol=execution.symbol, _id=execution._id, timestamp=execution.timestamp, side=execution.side,
            price=execution.price, size=execution.size,
            buy_child_order_acceptance_id=execution.buy_child_order_acceptance_id,
            sell_child_order_acceptance_id=execution.sell_child_order_acceptance_id,
            timeunit_if_ohlc_from=timeunit_if_ohlc_from,
            synchronized_execution_price_deviation=synchronized_execution and (
                    execution.price - synchronized_execution.price) / execution.price,
            synchronized_execution_time_delta=synchronized_execution and (
                    synchronized_execution.timestamp - execution.timestamp),
            synchronized_execution=synchronized_execution
        )


class SynchronizedExecution:

    def __init__(self,
                 symbol: Optional[Symbol] = None,
                 _id: Union[int, SwitchedToRealtime] = None,
                 timestamp: Optional[np.datetime64] = None,
                 side: Optional[Side] = None,
                 price: Optional[Decimal] = None,
                 size: Optional[Decimal] = None,
                 buy_child_order_acceptance_id: Optional[str] = None,
                 sell_child_order_acceptance_id: Optional[str] = None,
                 **attrs):
        self.symbol = symbol
        self._id = _id
        self.timestamp = timestamp
        self.side = side
        self.price = price
        self.size = size
        self.buy_child_order_acceptance_id = buy_child_order_acceptance_id
        self.sell_child_order_acceptance_id = sell_child_order_acceptance_id
        self.attrs = attrs

    def __str__(self):
        return f'{self.__class__.__name__}(symbol={self.symbol and self.symbol.value}' \
               f', _id={self._id and self._id}' \
               f', timestamp={self.timestamp and self.timestamp!s}' \
               f', side={self.side and self.side!s}' \
               f', price={self.price and self.price!s}' \
               f', size={self.size and self.size!s}' \
               f', buy_child_order_acceptance_id=' \
               f'{self.buy_child_order_acceptance_id and self.buy_child_order_acceptance_id!s}' \
               f', sell_child_order_acceptance_id=' \
               f'{self.sell_child_order_acceptance_id and self.sell_child_order_acceptance_id!s}' \
               f', attrs={self.attrs!s})'

    def __repr__(self):
        symbol = self.symbol and f'{Symbol.__name__}.{self.symbol.value!s}'
        return f'{self.__class__.__name__}(symbol={symbol}' \
               f', _id={self._id and self._id}' \
               f', timestamp={self.timestamp and self.timestamp!r}' \
               f', side={self.side and self.side!s}' \
               f', price={self.price and self.price!r}' \
               f', size={self.size and self.size!r}' \
               f', buy_child_order_acceptance_id=' \
               f'{self.buy_child_order_acceptance_id and self.buy_child_order_acceptance_id!r}' \
               f', sell_child_order_acceptance_id=' \
               f'{self.sell_child_order_acceptance_id and self.sell_child_order_acceptance_id!r}' \
               f', attrs={self.attrs!r})'

    def __eq__(self, other: 'SynchronizedExecution'):
        return self.symbol == other.symbol and \
               self._id == other._id and \
               self.timestamp == other.timestamp and \
               self.side == other.side and \
               self.price == other.price and \
               self.size == other.size and \
               self.buy_child_order_acceptance_id == other.buy_child_order_acceptance_id and \
               self.sell_child_order_acceptance_id == other.sell_child_order_acceptance_id and \
               self.attrs == other.attrs

    @staticmethod
    def from_execution(e: Execution):
        return SynchronizedExecution(
            symbol=e.symbol, _id=e._id, timestamp=e.timestamp, side=e.side, price=e.price, size=e.size,
            buy_child_order_acceptance_id=e.buy_child_order_acceptance_id,
            sell_child_order_acceptance_id=e.sell_child_order_acceptance_id,
            **e.attrs
        )
