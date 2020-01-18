import unittest

from trade.execution.tests import test_queue


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(test_queue.test_suite())
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='test_suite', verbosity=2, buffer=True)
