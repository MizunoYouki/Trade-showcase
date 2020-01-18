import unittest


def test_suite():
    from trade.strategy.tests import test_risk
    suite = unittest.TestSuite()
    suite.addTest(test_risk.test_suite())
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='test_suite', verbosity=2, buffer=True)
