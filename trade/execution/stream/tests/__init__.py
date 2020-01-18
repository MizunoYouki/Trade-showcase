import unittest

from trade.execution.stream.tests import test_chain, test_sqlite, test_s3


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(test_chain.test_suite())
    suite.addTest(test_sqlite.test_suite())
    suite.addTest(test_s3.test_suite())
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='test_suite', verbosity=2, buffer=True)
