import warnings
from decimal import Decimal
from enum import Enum
from logging import Logger

warnings.warn('TrendObserver is deprecated', DeprecationWarning)


# TODO: リファクタリング
class TrendObserver:
    class Direction(Enum):
        MarketFollowing = 1
        Contrarian = 0

    _logger: Logger
    _reversal_threshold_for_dd: Decimal

    def __init__(self,
                 logger: Logger,
                 reversal_threshold_draw_down_roc: Decimal):
        self._logger = logger
        self._threshold = reversal_threshold_draw_down_roc
        self._roc = Decimal('0.0')
        self._roc_high = Decimal('0.0')
        self._draw_down = Decimal('0.0')
        self.direction = self.Direction.MarketFollowing  # Initial is market-following

        self._logger.info('Trend observer initialized direction as market-following')

    def apply(self, roc: Decimal):
        self._roc += roc

        # When ROC high updated, clear the draw down
        if self._roc_high < self._roc:
            self._roc_high = self._roc
            self._draw_down = Decimal('0.0')

        else:
            self._draw_down = self._roc / self._roc_high

            # Reverse the trend direction
            if self._draw_down <= self._threshold:
                if self.direction is self.Direction.MarketFollowing:
                    self.direction = self.Direction.Contrarian
                else:
                    self.direction = self.Direction.MarketFollowing

                self._logger.info(f'Trend observer reverses direction to {self.direction}')

    def reverse_direction(self, dd_roc_snapshot):
        raise Exception('Deprecated')

        if not self.enabled:
            return

        self.market_following = not self.market_following
        self._draw_down = dd_roc_snapshot

        if self.market_following:
            self._logger.info('observer reverses direction: Following')
        else:
            self._logger.info('observer reverses direction: Contrarian')

    def guess_reversal(self, dd_roc):
        raise Exception('Deprecated')

        if not self.enabled:
            return False

        # When draw down recovered, also dd_roc_snapshot should be recovered
        if 0 <= dd_roc:
            self._draw_down = 0.0
            return False

        dd_roc_delta = dd_roc - self._draw_down
        if dd_roc_delta < self._reversal_threshold_for_dd:

            if self.market_following:
                self._logger.info('observer observed new trend: Contrarian')
            else:
                self._logger.info('observer observed new trend: Following')

            return True

        return False
