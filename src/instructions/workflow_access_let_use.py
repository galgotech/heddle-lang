from instructions.instruction import Instruction


class WorkflowAccessLetUseInstruction(Instruction):
    __workflow: str
    __let: str

    def __init__(self, workflow: str, name: str):
        super().__init__()
        self.__workflow = workflow
        self.__let = name

    @property
    def workflow(self) -> str:
        return self.__workflow

    @property
    def let(self) -> str:
        return self.__let

    def name(self) -> str:
        return "workflow_access_let_use"
