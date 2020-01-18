from abc import abstractmethod

from trade.execution.model import Execution
from trade.sign import Signal


class EndOfExecutions:
    """
    これ以上Executionがないことをあらわします
    """


class BaseStrategy:

    @abstractmethod
    def make_decision(self, execution: Execution) -> Signal:
        pass
