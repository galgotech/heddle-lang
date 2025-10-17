import unittest

from lark import Lark

from ast_interpreter.value import Value
from test.util import search_children


with open("grammar.lark", "r") as f:
    grammar = f.read()


class TestValue(unittest.TestCase):
    def setUp(self):
        self.parser = Lark(grammar, start="program", parser="earley")

    def tearDown(self):
        pass

    def test_value_primitives(self):
        ast = self.parser.parse("""
scope_1 {
    let variable1 = 123
    let variable2 = 1.1
    let variable3 = "test"
    let variable4 = "t"
    let variable5 = false
    let variable6 = true
    let variable7 = null
}
""")
        trees = search_children(ast, "value")
        value = Value()
        value.visit(trees[0])
        self.assertEqual(value.result, 123)

        value = Value()
        value.visit(trees[1])
        self.assertEqual(value.result, 1.1)

        value = Value()
        value.visit(trees[2])
        self.assertEqual(value.result, "test")

        value = Value()
        value.visit(trees[3])
        self.assertEqual(value.result, "t")

        value = Value()
        value.visit(trees[4])
        self.assertEqual(value.result, False)

        value = Value()
        value.visit(trees[5])
        self.assertEqual(value.result, True)

        value = Value()
        value.visit(trees[6])
        self.assertEqual(value.result, None)

    def test_value_dict(self):
        ast = self.parser.parse("""
scope_1 {
    let variable1 = {
        key1: 123,
        key2: 1.1,
        key3: "test",
        key4: "t",
        key5: false,
        key6: true,
        key7: null
    }
}
""")
        trees = search_children(ast, "value")
        value = Value()
        value.visit(trees[0])
        self.assertEqual(value.result, {
            "key1": 123,
            "key2": 1.1,
            "key3": "test",
            "key4": "t",
            "key5": False,
            "key6": True,
            "key7": None
        })

    def test_value_dict_deep(self):
        ast = self.parser.parse("""
scope_1 {
    let variable1 = {
        key1: [ 123 ],
        key2: { key1: 1.1 },
        key3: "test",
        key4: "t",
        key5: false,
        key6: true,
        key7: [null]
    }
}
""")
        trees = search_children(ast, "value")
        value = Value()
        value.visit(trees[0])
        self.assertEqual(value.result, {
            "key1": [123],
            "key2": {"key1": 1.1},
            "key3": "test",
            "key4": "t",
            "key5": False,
            "key6": True,
            "key7": [None]
        })

    def test_value_list(self):
        ast = self.parser.parse("""
scope_1 {
    let variable1 = [
        123,
        1.1,
        "test",
        "t",
        false,
        true,
        null
    ]
}
""")
        trees = search_children(ast, "value")
        value = Value()
        value.visit(trees[0])
        self.assertEqual(value.result, [
            123,
            1.1,
            "test",
            "t",
            False,
            True,
            None
        ])
