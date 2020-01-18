from enum import Enum


class Side(Enum):
    SELL = 'SELL'
    BUY = 'BUY'
    NOTHING = 'NOTHING'
    CONTINUE = 'HOLDING'


def counter_side(side: Side):
    if side is Side.SELL:
        return Side.BUY

    elif side is Side.BUY:
        return Side.SELL

    else:
        raise Exception(f'Could not determine counter side for {side}')
