import unittest
from unittest.mock import Mock
from lark import Tree

from ast_interpreter.variable_access import VariableAccess
from runtime.memory import Runtime


class TestVariableAccess(unittest.TestCase):
    def setUp(self):
        self.memory = Runtime()

    def test_visit_success(self):
        self.memory.set("my_workflow", {"my_var": 42})

        tree = Tree('variable_access', [
            Mock(value='my_workflow'),
            Mock(value='my_var')
        ])

        accessor = VariableAccess(self.memory)
        accessor.visit(tree)

        self.assertEqual(accessor.result, 42)

    def test_visit_workflow_not_defined(self):
        tree = Tree('variable_access', [
            Mock(value='non_existent_workflow'),
            Mock(value='my_var')
        ])

        accessor = VariableAccess(self.memory)

        with self.assertRaisesRegex(NameError, "Workflow 'non_existent_workflow' is not defined"):
            accessor.visit(tree)

    def test_visit_variable_not_defined(self):
        self.memory.set("my_workflow", {})

        tree = Tree('variable_access', [
            Mock(value='my_workflow'),
            Mock(value='non_existent_var')
        ])

        accessor = VariableAccess(self.memory)

        with self.assertRaisesRegex(NameError, "Variable 'non_existent_var' is not defined in workflow 'my_workflow'"):
            accessor.visit(tree)
