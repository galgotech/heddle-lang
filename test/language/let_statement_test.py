import unittest

from language.let_statement import LetStatement
from language.memory import Memory
from language.grammar import parse


class TestLetStatement(unittest.TestCase):
    def test_let_statement_with_value(self):
        # The code to test
        tree = parse('my_workflow { let a = 1 }')
        let_statement_node = next(tree.find_data("let_statement"))

        # The state of the interpreter
        mem = Memory()
        interpreter = LetStatement(mem, {})

        # Execute the let statement
        interpreter.visit(let_statement_node)

        # Assert the memory is in the correct state
        self.assertEqual(1, mem.get('a'))

    def test_let_statement_with_pipeline(self):
        # The code to test
        tree = parse('my_workflow { let a = b | mod.add_one ? }')
        let_statement_node = next(tree.find_data("let_statement"))

        # The state of the interpreter
        mem = Memory()
        mem.set('b', 1)
        modules = {'mod': {'add_one': lambda x: x + 1}}

        interpreter = LetStatement(mem, modules)

        # Execute the let statement
        interpreter.visit(let_statement_node)

        # Assert the memory is in the correct state
        self.assertEqual(2, mem.get('a'))
