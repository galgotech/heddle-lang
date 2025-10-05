import unittest
from unittest.mock import Mock

from language.scope import Scope
from language.memory import Memory
from language.grammar import parse


class TestAnonymousScope(unittest.TestCase):
    def setUp(self):
        self.memory = Memory()
        self.modules = {'my_module': Mock()}

    def test_let_statement_in_scope(self):
        tree = parse('my_workflow { { let a = 1 } }')
        anonymous_scope_node = next(tree.find_data("anonymous_scope"))

        scope = Scope(self.memory, {})
        result = scope.run(anonymous_scope_node)

        self.assertEqual(result, 1)
        self.assertFalse(self.memory.has('a'))

    def test_pipeline_statement_in_scope(self):
        tree = parse('my_workflow { { "input" | mod.up ? } }')
        anonymous_scope_node = next(tree.find_data("anonymous_scope"))
        modules = {'mod': {'up': lambda s: s.upper()}}

        scope = Scope(self.memory, modules)
        result = scope.run(anonymous_scope_node)

        self.assertEqual(result, "INPUT")

    def test_scope_return_value(self):
        tree = parse('test_workflow { { {} } }')
        anonymous_scope_node = next(tree.find_data('anonymous_scope'))

        scope = Scope(self.memory, self.modules)
        result = scope.run(anonymous_scope_node)

        self.assertEqual(result, {})

    def test_scope_return_variable(self):
        self.memory.set('my_var', 123)

        tree = parse('test_workflow { { my_var } }')
        anonymous_scope_node = next(tree.find_data('anonymous_scope'))

        scope = Scope(self.memory, self.modules)
        result = scope.run(anonymous_scope_node)

        self.assertEqual(result, 123)

    def test_nested_anonymous_scope(self):
        tree = parse('test_workflow { { { {} } } }')
        anonymous_scope_node = next(tree.find_data('anonymous_scope'))

        scope = Scope(self.memory, self.modules)
        result = scope.run(anonymous_scope_node)

        self.assertEqual(result, {})

    def test_memory_scoping(self):
        self.memory.set('outer_var', 'outer')

        tree = parse('my_workflow { { let inner_var = 123 } }')
        anonymous_scope_node = next(tree.find_data("anonymous_scope"))

        scope = Scope(self.memory, self.modules)
        result = scope.run(anonymous_scope_node)

        self.assertEqual(result, 123)
        self.assertTrue(self.memory.has('outer_var'))
        self.assertFalse(self.memory.has('inner_var'))

    def test_scope_returns_pipeline_result(self):
        tree = parse('my_workflow { { "hello" | mod.upper ? } }')
        anonymous_scope_node = next(tree.find_data("anonymous_scope"))

        # The state of the interpreter
        modules = {'mod': {'upper': lambda x: x.upper()}}

        # Execute the anonymous scope
        scope = Scope(self.memory, modules)
        result = scope.run(anonymous_scope_node)

        # Assert the result is correct
        self.assertEqual("HELLO", result)

    def test_scope_returns_let_statement_result(self):
        tree = parse('my_workflow { { let a = 42 } }')
        anonymous_scope_node = next(tree.find_data("anonymous_scope"))

        # Execute the anonymous scope
        scope = Scope(self.memory, {})
        result = scope.run(anonymous_scope_node)

        # Assert the result is correct
        self.assertEqual(42, result)
