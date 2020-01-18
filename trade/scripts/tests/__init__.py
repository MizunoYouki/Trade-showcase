import unittest

from trade.scripts.tests import test_dataset


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(test_dataset.test_suite())
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='test_suite', verbosity=2, buffer=True)
