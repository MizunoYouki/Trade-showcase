import unittest


def test_suite():
    import trade.executionwriter.tests
    import trade.execution.stream.tests
    import trade.execution.stream.adapter.tests
    import trade.execution.tests
    import trade.broker.httpclient.tests
    import trade.broker.declarative.tests
    import trade.broker.declarative.bitflyer.tests
    import trade.strategy.tests
    suite = unittest.TestSuite()
    suite.addTest(trade.executionwriter.tests.test_suite())
    suite.addTest(trade.execution.stream.tests.test_suite())
    suite.addTest(trade.execution.stream.adapter.tests.test_suite())
    suite.addTest(trade.execution.tests.test_suite())
    suite.addTest(trade.broker.httpclient.tests.test_suite())
    suite.addTest(trade.broker.declarative.tests.test_suite())
    suite.addTest(trade.broker.declarative.bitflyer.tests.test_suite())
    suite.addTest(trade.strategy.tests.test_suite())
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='test_suite', verbosity=2, buffer=True)
