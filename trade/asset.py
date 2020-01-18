import dataclasses
from decimal import Decimal
from logging import Logger
from typing import Union

import numpy as np

from trade.model import Position, Trade
from trade.side import Side
from trade.strategy.internal.risk import RocDrawDown


# TODO: リファクタリング
class Asset:
    @dataclasses.dataclass
    class Stat:
        num_trade_total: int = 0
        num_trade_profit: int = 0
        probability_of_win: Decimal = Decimal('0.0')
        sum_profit: Decimal = Decimal('0')
        sum_loss: Decimal = Decimal('0')
        profit_sigma: Decimal = Decimal('0')
        profit_factor: Decimal = Decimal('0.0')

    _logger: Logger
    _log_format: str
    _position: Position
    _last_origin_at: Union[np.datetime64, None]
    price: Decimal
    roc_total: Decimal
    stat: 'Stat'

    def __init__(self, logger: Logger, log_format: str = 'plain'):
        self._logger = logger
        self._log_format = log_format

        # self._position = Position.NoPosition
        self._last_origin_at = None
        self._dd_roc = RocDrawDown(logger=logger)

        self.price = Decimal('0')
        self.roc_total = Decimal('0.0')
        self.stat = self.Stat()

        if self._log_format.lower() == 'tsv':
            self._logger.info('\t'.join(Trade.columns_playback()))

    def new_long_position(self, price: Decimal, decision_at: np.datetime64):
        if not self.price.is_zero():
            self._dd_roc.finish_period()
            dd_roc = Decimal('0')
        else:
            dd_roc = self._dd_roc.get_value()
        self._dd_roc.start_period(held_side=Side.BUY, initial_price=price, initial_roc=dd_roc)

        self._new_position(price, Position.Long, decision_at)

    def new_short_position(self, price: Decimal, decision_at: np.datetime64):
        if self.price.is_zero():
            self._dd_roc.finish_period()
            dd_roc = Decimal('0')
        else:
            dd_roc = self._dd_roc.get_value()
        self._dd_roc.start_period(held_side=Side.SELL, initial_price=price, initial_roc=dd_roc)

        self._new_position(price, Position.Short, decision_at)

    def close_position(self, price: Decimal,
                       origin_at: np.datetime64,
                       decision_at: np.datetime64) -> Decimal:
        """
        ポジションをクローズします
        :param price:
        :param origin_at:
        :param decision_at:
        :return: クローズされたトレードのROC
        """
        profit = self._calc_profit(price)

        if not profit.is_zero() or not self.price.is_zero():
            roc = profit / self.price
            self.roc_total += roc

            self._dd_roc.apply(price)
            roc_drop = self._dd_roc.get_value()
        else:
            roc = Decimal('0.0')
            roc_drop = Decimal('0.0')

        if 0 < profit:
            self.stat.num_trade_profit += 1
            self.stat.sum_profit += profit
        if profit < 0:
            self.stat.sum_loss += profit

        self.stat.profit_sigma += profit

        if self.stat.sum_loss.is_zero():
            self.stat.profit_factor = np.nan
        else:
            self.stat.profit_factor = self.stat.sum_profit / (self.stat.sum_loss * Decimal('-1'))

        self.stat.num_trade_total += 1
        self.stat.probability_of_win = Decimal(self.stat.num_trade_profit) / Decimal(self.stat.num_trade_total)

        if self._last_origin_at:
            hold_in_ns = (decision_at - self._last_origin_at).item() / 1_000_000_000
            hold_in_min = int((decision_at.item() / 1_000_000_000) // 60) - int(
                (self._last_origin_at.item() / 1_000_000_000) // 60)
        else:
            hold_in_ns = np.nan
            hold_in_min = np.nan
        self._last_origin_at = decision_at

        trade = Trade(
            origin_at=origin_at,
            decision_at=decision_at,
            profit=profit,
            profit_sigma=self.stat.profit_sigma,
            position=self._position,
            entry=self.price,
            exit=price,
            roc_total=self.roc_total,
            profit_factor=self.stat.profit_factor,
            probability_of_win=self.stat.probability_of_win,
            roc_this_trade=roc,
            draw_down=roc_drop,
            reversal=False,
            hold_in_nanoseconds=hold_in_ns,
            hold_in_minutes=hold_in_min
        )

        if self._log_format.lower() == 'tsv':
            self._logger.info('\t'.join([str(v) for v in trade.fields_playback()]))

        else:
            # TODO: use dataclass variables
            self._logger.info(
                'close asset (at:{}, profit:{}, profit(sigma):{}, position:{}, entry:{}, exit:{}, roc(total):{:.4%}'
                ', pl:{:.3}, pf_trade_ratio: {:.2%}, roc(indiv):{:.4%}, dd:{:.4%}, reversal:{}'
                ', hold:{}, hold_mins:{})'.format(
                    decision_at, profit, self.stat.profit_sigma, self._position, self.price, price,
                    self.roc_total, self.stat.profit_factor, self.stat.probability_of_win, roc, roc_drop,
                    '0',
                    hold_in_ns, hold_in_min
                )
            )

        self.price = price
        self._position = Position.NoPosition

        return roc

    def _new_position(self, price: Decimal, position: Position, decision_at: np.datetime64):
        if self._log_format.lower() == 'plain':
            self._logger.info('open asset ({}, entry:{}, decision_at:{})'.format(position, price, decision_at))

        self.price = price
        self._position = position

    def _calc_profit(self, price: Decimal):
        if self._position is Position.NoPosition:
            return Decimal('0')
        elif self._position is Position.Long:
            return price - self.price
        elif self._position is Position.Short:
            return self.price - price
        else:
            raise Exception('Unexpected position: {}'.format(self._position))

    def __str__(self):
        if self._log_format.lower() == 'plain':
            return ''
        else:
            return 'ROC: {:.4%}, DD: TODO, NTRADES: {}, PF_TRADE_RATIO: {:.2%}, PL:{:.3}'.format(
                self.roc_total, self.stat.num_trade_total, self.stat.probability_of_win, self.stat.profit_factor
            )
