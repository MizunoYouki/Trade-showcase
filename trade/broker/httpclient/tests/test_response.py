import unittest
from decimal import Decimal

from trade.broker.httpclient.response import BaseResponse


class BaseResponseTestCase(unittest.TestCase):
    class MockResponse(BaseResponse):
        id: int
        size: Decimal

    def setUp(self):
        raw = {
            'id': 123,
            'size': 4.5,
            'foo': 'bar',
            'spam': 5.67
        }

        self._response = self.MockResponse(raw)

    def test_init(self):
        self.assertEqual(int, type(self._response.id))
        self.assertEqual(123, self._response.id)
        self.assertEqual(Decimal, type(self._response.size))
        self.assertEqual(Decimal('4.5'), self._response.size)

        # noinspection PyUnresolvedReferences
        self.assertEqual(str, type(self._response.foo))
        # noinspection PyUnresolvedReferences
        self.assertEqual('bar', self._response.foo)

        # noinspection PyUnresolvedReferences
        self.assertEqual(Decimal, type(self._response.spam))
        # noinspection PyUnresolvedReferences
        self.assertEqual(Decimal('5.67'), self._response.spam)

    def test_repr(self):
        self.assertEqual(
            "MockResponse(id=123, size=Decimal('4.5'), foo='bar', spam=Decimal('5.67'))",
            repr(self._response)
        )

    def test_str(self):
        self.assertEqual(
            "MockResponse(id=123, size=4.5, foo=bar, spam=5.67)",
            str(self._response)
        )

    def test_init_empty(self):
        dict_obj = {}
        model = self.MockResponse(dict_obj)

        self.assertEqual(
            "MockResponse()",
            repr(model)
        )

        self.assertEqual(
            "MockResponse()",
            str(model)
        )

    def test_init_nested(self):
        dict_obj = {
            'id': 123,
            'size': 4.5,
            'foo': 'bar',
            'child': {
                'id': 124,
                'ham': 0.12,
                'grandchild': {
                    'size': 0.890,
                    'spam': 'ham'
                }
            }
        }
        model = self.MockResponse(dict_obj)

        self.assertEqual(
            "MockResponse(id=123, size=Decimal('4.5'), foo='bar'"
            ", child={'id': 124, 'ham': 0.12, 'grandchild': {'size': 0.89, 'spam': 'ham'}})",
            repr(model)
        )

        self.assertEqual(
            "MockResponse(id=123, size=4.5, foo=bar"
            ", child={'id': 124, 'ham': 0.12, 'grandchild': {'size': 0.89, 'spam': 'ham'}})",
            str(model)
        )


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(BaseResponseTestCase))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='test_suite', verbosity=2, buffer=True)
