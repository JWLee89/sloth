from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from sloth.validation.basic import is_list_of


class Task(ABC):
    """
    Abstract base class for a single unit of operation in the flow (a node in the DAG).
    Users should inherit from this class and implement the run method.
    Each task must have:
        - name: Unique identifier
        - input_tasks: List of names of tasks this task depends on
    """

    def __init__(self, name: str = None, input_tasks: Optional[List[str]] = None):
        """Basic initialization of a task.

        Args:
            name (str, optional): The name of the task.
            input_tasks (Optional[List[str]], optional): _description_. Defaults to None.
        """
        if name is None:
            name = type(self).__name__
        # TODO: refactor this to use a more robust type checking library
        if not isinstance(name, str):
            raise TypeError("name of a task must be a string")
        if not is_list_of(input_tasks, str):
            raise ValueError("input_tasks must be a list of strings")
        self.name = name
        self.input_tasks = input_tasks or []

    @abstractmethod
    def run(self, inputs: Dict[str, Any]) -> Any:
        """
        Executes the task's operation. Must be implemented by subclasses.
        Args:
            inputs: Dict mapping input task names to their outputs.
        Returns:
            The result of the task's operation.
        """
        pass
