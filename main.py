from typing import Any, Dict, List, Optional
from sloth.dag.flow import Flow
from sloth.dag.task import Task


def main():
    """Main function to run the example."""

    # Example user-defined tasks
    class AddTask(Task):
        """Sum all input values and add a constant value."""

        def __init__(
            self,
            name: str = None,
            input_tasks: Optional[List[str]] = None,
            add_value: int = 0,
        ):
            """Initialize the AddTask."""
            super().__init__(name, input_tasks)
            self.add_value = add_value

        def run(self, inputs: Dict[str, Any]) -> Any:
            """Sums all input values and adds self.add_value"""
            return sum(inputs.values()) + self.add_value

    class MultiplyTask(Task):
        def __init__(
            self,
            name: str = None,
            input_tasks: Optional[List[str]] = None,
            factor: int = 1,
        ):
            """Initialize the MultiplyTask."""
            super().__init__(name, input_tasks)
            self.factor = factor

        def run(self, inputs: Dict[str, Any]) -> Any:
            """Multiply all input values by self.factor"""
            result = 1
            for v in inputs.values():
                result *= v
            return result * self.factor

    # Create a flow containing a cycle for testing
    # It should raise a ValueError if a cycle is detected.
    a = AddTask(name="A")
    b = AddTask(name="B")
    c = MultiplyTask(name="C", input_tasks=["A", "B"], factor=10)
    d = AddTask(name="D", input_tasks=["C"], add_value=1)

    flow = Flow(name="test", tasks=[a, b, c, d])
    print(flow.visualize())
    print("Flowchart-style DAG visualization:")
    try:
        result = flow.run(2, 3)
        print(result)
    except ValueError as e:
        print(e)


if __name__ == "__main__":
    main()
