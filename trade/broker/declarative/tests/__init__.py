import unittest

from trade.broker.declarative.tests import test_model


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(test_model.test_suite())
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='test_suite', verbosity=2, buffer=True)
