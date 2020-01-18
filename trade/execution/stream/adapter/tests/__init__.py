import unittest

from trade.execution.stream.adapter.tests import test_filter, test_sync


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(test_filter.test_suite())
    suite.addTest(test_sync.test_suite())
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='test_suite', verbosity=2, buffer=True)
