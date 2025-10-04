import sys
from lark import Lark

from language import LanguageInterpreter

if __name__ == '__main__':
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
    else:
        file_path = "example.he"

    with open("language/grammar.lark", "r") as f:
        grammar = f.read()
    with open(file_path, "r") as f:
        code = f.read()

    parser = Lark(grammar, start='program', parser='earley')
    ast = parser.parse(code)

    interpreter = LanguageInterpreter()
    interpreter.visit(ast)
    print(interpreter.memory.current_scope)