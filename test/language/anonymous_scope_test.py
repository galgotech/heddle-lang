import unittest
from unittest.mock import Mock
from lark import Tree

from language.anonymous_scope import AnonymousScope
from language.memory import Memory
from language.grammar import parse

class TestAnonymousScope(unittest.TestCase):
    def setUp(self):
        self.memory = Memory()
        self.modules = {'my_module': Mock()}

    def test_let_statement_in_scope(self):
        # Mocking LetStatement to isolate AnonymousScope logic
        with unittest.mock.patch('language.anonymous_scope.LetStatement') as MockLetStatement:
            instance = MockLetStatement.return_value

            tree = Tree('anonymous_scope', [
                Tree('let_statement', [])
            ])

            scope = AnonymousScope(self.memory, self.modules)
            scope.visit(tree)

            MockLetStatement.assert_called_once_with(self.memory, self.modules)
            instance.visit.assert_called_once()

    def test_pipeline_statement_in_scope(self):
        # Mocking PipeLineStatement to isolate AnonymousScope logic
        with unittest.mock.patch('language.anonymous_scope.PipeLineStatement') as MockPipeLineStatement:
            instance = MockPipeLineStatement.return_value

            tree = Tree('anonymous_scope', [
                Tree('pipeline_statement', [])
            ])

            scope = AnonymousScope(self.memory, self.modules)
            scope.visit(tree)

            MockPipeLineStatement.assert_called_once_with(self.memory, self.modules)
            instance.visit.assert_called_once()

    def test_scope_return_value(self):
        tree = Tree('anonymous_scope', [
            Tree('scope_return', [
                Tree('value', [Tree('dict', [])])
            ])
        ])

        scope = AnonymousScope(self.memory, self.modules)
        scope.visit(tree)

        self.assertEqual(scope.result, {})

    def test_scope_return_variable(self):
        self.memory.set('my_var', 123)

        tree = Tree('anonymous_scope', [
            Tree('scope_return', [
                Mock(type='VARIABLE_NAME', value='my_var')
            ])
        ])

        scope = AnonymousScope(self.memory, self.modules)
        scope.visit(tree)

        self.assertEqual(scope.result, 123)

    def test_nested_anonymous_scope(self):
        nested_scope_tree = Tree('anonymous_scope', [
            Tree('scope_return', [
                Tree('value', [Tree('dict', [])])
            ])
        ])
        tree = Tree('anonymous_scope', [nested_scope_tree])

        scope = AnonymousScope(self.memory, self.modules)
        scope.visit(tree)

        self.assertEqual(scope.result, {})

    def test_memory_scoping(self):
        self.memory.set('outer_var', 'outer')

        tree = parse('my_workflow { { let inner_var = 123 } }')
        anonymous_scope_node = next(tree.find_data("anonymous_scope"))

        scope = AnonymousScope(self.memory, self.modules)
        scope.visit(anonymous_scope_node)

        self.assertEqual(scope.result, 123)
        self.assertTrue(self.memory.has('outer_var'))
        self.assertFalse(self.memory.has('inner_var'))