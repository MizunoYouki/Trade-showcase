from dataclasses import dataclass
from decimal import Decimal
from logging import Logger
from typing import AsyncIterable

from trade.asset import Asset
from trade.execution.model import Execution
from trade.model import Position
from trade.side import Side
from trade.sign import Signal
from trade.strategy import BaseStrategy


async def stub_broker(logger: Logger,
                      reader: AsyncIterable[Execution],
                      strategy: BaseStrategy,
                      losscut: Decimal):
    """
    必ず約定する理想的なbroker
    """

    @dataclass
    class _Entered:
        position: Position
        price: Decimal

    execution: Execution
    asset: Asset = Asset(logger, log_format='tsv')  # TODO: Assetがreversalを正しく表示できるように（いまは常にFalse）
    entered: _Entered = _Entered(position=Position.NoPosition, price=Decimal('NaN'))

    async for execution in reader:
        signal: Signal = strategy.make_decision(execution)

        if entered.position is Position.NoPosition:
            if signal.side is Side.BUY:
                asset.new_long_position(signal.price, signal.decision_at)
                entered = _Entered(position=Position.Long, price=signal.price)

            elif signal.side is Side.SELL:
                asset.new_short_position(signal.price, signal.decision_at)
                entered = _Entered(position=Position.Short, price=signal.price)

        elif entered.position is Position.Long:

            profit = execution.price - entered.price
            if profit <= losscut:
                losscut_price = entered.price + losscut
                asset.close_position(losscut_price, origin_at=signal.origin_at, decision_at=signal.decision_at)
                entered = _Entered(position=Position.NoPosition, price=Decimal('NaN'))

            if signal.side is Side.SELL:
                asset.close_position(signal.price, origin_at=signal.origin_at, decision_at=signal.decision_at)
                asset.new_short_position(signal.price, signal.decision_at)
                entered = _Entered(position=Position.Short, price=signal.price)

            elif signal.side is Side.NOTHING:
                asset.close_position(signal.price, origin_at=signal.origin_at, decision_at=signal.decision_at)
                entered = _Entered(position=Position.NoPosition, price=signal.price)

        elif entered.position is Position.Short:

            profit = entered.price - execution.price
            if profit <= losscut:
                losscut_price = entered.price - losscut
                asset.close_position(losscut_price, origin_at=signal.origin_at, decision_at=signal.decision_at)
                entered = _Entered(position=Position.NoPosition, price=Decimal('NaN'))

            if signal.side is Side.BUY:
                asset.close_position(signal.price, origin_at=signal.origin_at, decision_at=signal.decision_at)
                asset.new_long_position(signal.price, signal.decision_at)
                entered = _Entered(position=Position.Long, price=signal.price)

            elif signal.side is Side.NOTHING:
                asset.close_position(signal.price, origin_at=signal.origin_at, decision_at=signal.decision_at)
                entered = _Entered(position=Position.NoPosition, price=signal.price)
