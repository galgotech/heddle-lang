import unittest

from test.ast_interpreter.value_test import TestValue
from test.ast_interpreter.let_statement_test import TestLetStatement
from test.ast_interpreter.variable_access_test import TestVariableAccess
from test.ast_interpreter.anonymous_scope_test import TestAnonymousScope
from test.ast_interpreter.prql_test import TestPrql


def create_suite():
    suite = unittest.TestSuite()
    loader = unittest.TestLoader()
    suite.addTest(loader.loadTestsFromTestCase(TestValue))
    suite.addTest(loader.loadTestsFromTestCase(TestLetStatement))
    suite.addTest(loader.loadTestsFromTestCase(TestVariableAccess))
    suite.addTest(loader.loadTestsFromTestCase(TestAnonymousScope))
    suite.addTest(loader.loadTestsFromTestCase(TestPrql))

    return suite


if __name__ == '__main__':
    # Create the suite of tests
    suite = create_suite()

    # Create a TextTestRunner with a higher verbosity level for more detailed output
    runner = unittest.TextTestRunner(verbosity=2)

    # Run the test suite
    print("Running a custom test suite...\n")
    runner.run(suite)
