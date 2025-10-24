import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from runtime.dependency_graph import DependencyGraph
from runtime.local import Runtime


class Executor:
    def __init__(self, graph: DependencyGraph, runtime: Runtime):
        self.graph = graph
        self.runtime = runtime
        self.lock = threading.Lock()
        self.completed_instructions = set()

    def execute(self):
        with ThreadPoolExecutor() as executor:
            while len(self.completed_instructions) < len(self.graph.instructions):
                ready_instructions = self.get_ready_instructions()
                futures = {executor.submit(self.execute_instruction, inst): inst for inst in ready_instructions}

                for future in as_completed(futures):
                    instruction = futures[future]
                    try:
                        future.result()
                        self.mark_as_complete(instruction)
                    except Exception as e:
                        print(f"Error executing instruction {instruction.id}: {e}")
                        # Handle error, maybe stop execution
                        return

    def execute_instruction(self, instruction):
        # The instruction's execute method needs to be adapted
        # to handle the new instruction format.
        instruction.execute(self.runtime)

    def get_ready_instructions(self):
        with self.lock:
            ready = []
            for inst in self.graph.instructions:
                if inst in self.completed_instructions:
                    continue
                
                if all(dep in self.completed_instructions for dep in self.graph.dependencies[inst]):
                    ready.append(inst)
            return ready

    def mark_as_complete(self, instruction):
        with self.lock:
            self.completed_instructions.add(instruction)