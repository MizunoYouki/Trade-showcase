from dataclasses import dataclass
from decimal import Decimal
from typing import Mapping, Any

from trade.model import Symbol
from trade.side import Side


@dataclass
class BaseResponse:
    """
    基底HTTPレスポンス

    このクラスを直接、または継承して、HTTPレスポンスをモデル化します。
    class Response(AbstractResponse):
        id: int
        size: Decimal

    float型の定義は、Decimal型に変換されます。
    r = Response({
        ...
        'spam': 5.67
    })
    print(type(r.spam), r.spam)  # <class 'Decimal'> 5.67

    モデル化されたクラスのコンストラクタに未定義の属性が渡された場合は、属性が追加されます。
    r = Response({
        'id': 123,
        'size': 4.5,
        'spam': 5.67,
        'foo': 'bar',
    })
    print(type(r.id), r.id)  # <class 'int'> 123
    print(type(r.size), r.size)  # <class 'decimal.Decimal'> 4.5
    print(type(r.spam), r.spam)  # <class 'Decimal'> 5.67
    print(type(r.foo), r.foo)  # <class 'str'> 'bar'
    """

    def __init__(self, raw: Mapping[str, Any]):
        if hasattr(self, '__annotations__'):
            for k, v in raw.items():
                if k in self.__annotations__:
                    if isinstance(v, float):
                        v = Decimal(str(v))
                        setattr(self, k, v)
                        continue

                    setattr(self, k, self.__annotations__[k](v))

                else:
                    if isinstance(v, float):
                        v = Decimal(str(v))
                    setattr(self, k, v)
        else:
            for k, v in raw.items():
                setattr(self, k, v)

    def __repr__(self):
        kvs = dict()
        for k, v in self.__dict__.items():
            _type = type(v)
            if _type in (Symbol, Side):
                kvs[k] = f'{_type.__name__}.{v.value}'
            else:
                kvs[k] = repr(v)

        return f'{self.__class__.__name__}({", ".join(f"{k}={v}" for k, v in kvs.items())})'

    def __str__(self):
        return f'{self.__class__.__name__}({self._str_args()})'

    def _str_args(self) -> str:
        return ", ".join(f"{k}={type(v) == Symbol and v.value or v!s}" for k, v in self.__dict__.items())


class FallbackMixin:
    _fallback: BaseResponse

    def get_fallback(self):
        return self._fallback
