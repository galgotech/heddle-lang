import logging
import sys
from lark import Lark
from ast_interpreter.start import Start
from runtime.local import Runtime


class TreeFormatter(logging.Formatter):
    def format(self, record):
        indent = getattr(record, 'indent', 0)
        indent_str = '  ' * indent
        record.msg = f"{indent_str}{record.msg}"
        return super().format(record)


logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s:%(name)s - %(message)s',
    # format='%(asctime)s - %(levelname)s - %(message)s'
)

for h in logging.root.handlers[:]:
    logging.root.removeHandler(h)

handler = logging.StreamHandler()
formatter = TreeFormatter('%(message)s')
handler.setFormatter(formatter)
logging.root.addHandler(handler)


if __name__ == '__main__':
    file_path = sys.argv[1]
    with open("src/grammar.lark", "r") as f:
        grammar = f.read()

    with open(file_path, "r") as f:
        code = f.read()

    parser = Lark(grammar, start='program', parser='earley')
    ast = parser.parse(code)

    runtime = Runtime()

    interpreter = Start(runtime)
    interpreter.visit(ast)
