import unittest
import polars as pl
from language.grammar import parse
from language.interpreter import LanguageInterpreter


class TestPrql(unittest.TestCase):
    def test_prql_pipeline(self):
        interpreter = LanguageInterpreter()
        code = """import "polars" pl
my_workflow {
    let df = [
        {a: 1, b: "x"},
        {a: 2, b: "y"},
        {a: 4, b: "z"}
    ] | pl.from_records?

    let result = df | (
        filter a > 1
        select {a}
    )?

    result
}"""
        tree = parse(code)
        interpreter.visit(tree)

        # The workflow returns the 'result' dataframe, which is then assigned
        # to a variable named after the workflow itself ('my_workflow').
        result_df = interpreter.memory.get('my_workflow')

        # Manually create the expected DataFrame for comparison
        expected_df = pl.DataFrame({
            "a": [2, 4],
        })

        # Assert that the resulting DataFrame is equal to the expected DataFrame
        self.assertTrue(result_df.equals(expected_df))
