import unittest

from trade.executionwriter.tests import test_sqlite


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(test_sqlite.test_suite())
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='test_suite', verbosity=2, buffer=True)
