import unittest
import polars as pl
from language.pipeline_statement import PipelineStatement
from language.memory import Memory
from language.grammar import parse


class TestPipelineStatement(unittest.TestCase):
    def test_pipeline_with_initial_value(self):
        # The code to test
        tree = parse('my_workflow { 1 | mod.add_one ? }')
        pipeline_statement_node = next(tree.find_data("pipeline_statement"))

        # The state of the interpreter
        mem = Memory()
        modules = {'mod': {'add_one': lambda x: x + 1}}

        interpreter = PipelineStatement(mem, modules)

        # Execute the pipeline statement
        result = interpreter.visit(pipeline_statement_node)

        # Assert the result is correct
        self.assertEqual(2, result)

    def test_pipeline_with_initial_variable(self):
        # The code to test
        tree = parse('my_workflow { a | mod.add_one ? }')
        pipeline_statement_node = next(tree.find_data("pipeline_statement"))

        # The state of the interpreter
        mem = Memory()
        mem.set('a', 5)
        modules = {'mod': {'add_one': lambda x: x + 1}}

        interpreter = PipelineStatement(mem, modules)

        # Execute the pipeline statement
        result = interpreter.visit(pipeline_statement_node)

        # Assert the result is correct
        self.assertEqual(6, result)

    def test_pipeline_with_multiple_functions(self):
        # The code to test
        tree = parse('my_workflow { 1 | mod.add_one | mod.double ? }')
        pipeline_statement_node = next(tree.find_data("pipeline_statement"))

        # The state of the interpreter
        mem = Memory()
        modules = {
            'mod': {
                'add_one': lambda x: x + 1,
                'double': lambda x: x * 2
            }
        }

        interpreter = PipelineStatement(mem, modules)

        # Execute the pipeline statement
        result = interpreter.visit(pipeline_statement_node)

        # Assert the result is correct
        self.assertEqual(4, result)

    def test_pipeline_with_prql_statement(self):
        # The code to test
        prql_query = "from employees | select {name, age}"
        tree = parse(f'my_workflow {{ | ({prql_query}) ? }}')
        pipeline_statement_node = next(tree.find_data("pipeline_statement"))

        # The state of the interpreter
        mem = Memory()
        employees_df = pl.DataFrame({
            "name": ["Alice", "Bob", "Charlie"],
            "age": [25, 30, 35],
            "department": ["HR", "Engineering", "Sales"]
        })
        mem.set("employees", employees_df)
        modules = {}

        interpreter = PipelineStatement(mem, modules)

        # Execute the pipeline statement
        result_df = interpreter.visit(pipeline_statement_node)

        # Assert the result is correct
        expected_df = pl.DataFrame({
            "name": ["Alice", "Bob", "Charlie"],
            "age": [25, 30, 35]
        })
        self.assertTrue(result_df.equals(expected_df))
