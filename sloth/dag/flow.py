from typing import Dict, List, Any, Set

from sloth.dag.task import Task


class Flow:
    """
    Represents a DAG of tasks.
    Provides methods to compile (cycle check), run, and visualize the flow.
    """

    def __init__(self, name: str, tasks: List[Task]):
        """
        Initialize the flow.

        Args:
            name (str): The name of the flow.
            tasks (List[Task]): The list of tasks in the flow.
        """
        self.name = name
        self.tasks: Dict[str, Task] = {task.name: task for task in tasks}
        self._compiled = False
        self._topo_order: List[str] = []
        self.root_tasks: List[str] = [t.name for t in tasks if not t.input_tasks]

    def compile(self) -> None:
        """
        Checks for cycles in the DAG using DFS. Raises ValueError if a cycle is found.
        Sets the topological order for execution.
        """
        visited: Set[str] = set()
        temp_mark: Set[str] = set()
        order: List[str] = []

        def visit(node: str, path: List[str]):
            """
            Helper function to visit a node in the DAG.
            """
            if node in temp_mark:
                # Find the cycle path
                if node in path:
                    cycle_start = path.index(node)
                    cycle_path = path[cycle_start:] + [node]
                else:
                    cycle_path = path + [node]
                raise ValueError(f"Cycle detected: {' -> '.join(cycle_path)}")
            if node not in visited:
                temp_mark.add(node)
                for dep in self.tasks[node].input_tasks:
                    if dep not in self.tasks:
                        raise ValueError(
                            f"Task '{dep}' (dependency of '{node}') not found in flow."
                        )
                    visit(dep, path + [node])
                temp_mark.remove(node)
                visited.add(node)
                order.append(node)

        for task_name in self.tasks:
            visit(task_name, [])
        self._topo_order = order
        self._compiled = True

    def run(self, *root_inputs) -> Dict[str, Any]:
        """
        Executes the flow in topological order.
        Args:
            *root_inputs: Values for root nodes, in the order of their appearance in the flow.
        Returns:
            Dict mapping leaf task names to their outputs.
        """
        if not self._compiled:
            self.compile()
        results: Dict[str, Any] = {}
        # Identify root and leaf nodes
        all_inputs = set()
        for task in self.tasks.values():
            all_inputs.update(task.input_tasks)
        root_tasks = self.root_tasks
        leaf_tasks = [name for name in self.tasks if name not in all_inputs]
        # Assign root inputs
        if len(root_inputs) != len(root_tasks):
            raise ValueError(
                f"Expected {len(root_tasks)} root inputs, got {len(root_inputs)}"
            )
        for i, name in enumerate(root_tasks):
            results[name] = root_inputs[i]
        # Run tasks in topological order
        for name in self._topo_order:
            if name in root_tasks:
                continue  # Already set
            task = self.tasks[name]
            input_dict = {dep: results[dep] for dep in task.input_tasks}
            output = task.run(input_dict)
            results[name] = output
        # Leaf outputs
        return {name: results[name] for name in leaf_tasks}

    def visualize(self) -> str:
        """
        Returns a human-readable formatted string representing the DAG structure.
        Each line shows a task and its dependencies.

        Returns:
            A string representing the DAG structure.
        """
        lines = [f"Flow: {self.name}"]
        for task_name in sorted(self.tasks):
            task = self.tasks[task_name]
            if task.input_tasks:
                deps = ", ".join(task.input_tasks)
                lines.append(f"  {task_name} <- [{deps}]")
            else:
                lines.append(f"  {task_name} (root)")
        return "\n".join(lines)
