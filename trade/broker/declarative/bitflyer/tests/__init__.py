import unittest

from trade.broker.declarative.bitflyer.tests import test_bitflyer, test_model, test_response


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(test_bitflyer.test_suite())
    suite.addTest(test_model.test_suite())
    suite.addTest(test_response.test_suite())
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='test_suite', verbosity=2, buffer=True)
