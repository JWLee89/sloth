from typing import Any, Type


def is_list_of(obj: Any, type: Type) -> bool:
    """Check if the object is a list of the given type.

    Args:
        obj (Any): The object to check.
        type (Type): The type to check against.

    Returns:
        bool: True if the object is a list of the given type, False otherwise.
    """
    return isinstance(obj, list) and all(isinstance(item, type) for item in obj)
