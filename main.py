from lark import Lark

from language import LanguageInterpreter

if __name__ == '__main__':
    with open("language/grammar.lark", "r") as f:
        grammar = f.read()
    with open("example.gojo", "r") as f:
        code = f.read()

    parser = Lark(grammar, start='program', parser='earley')
    ast = parser.parse(code)

    interpreter = LanguageInterpreter()
    interpreter.visit(ast)
    print(interpreter.memory.current_scope)