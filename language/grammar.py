import os
from lark import Lark

grammar_path = os.path.join(os.path.dirname(__file__), 'grammar.lark')

with open(grammar_path, 'r') as f:
    grammar = f.read()

parser = Lark(grammar, start='start')

parse = parser.parse