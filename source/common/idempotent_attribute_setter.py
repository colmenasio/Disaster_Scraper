from typing import Any, Callable


def idempotent_attribute_setter(attribute_name: str):
    """
    Decorator that only executes the callable if the attribute passed to it is not none

    Useful for initialization of instances and avoinding redundant calls
    """

    def inner(func: Callable[[Any], None]):
        def wrapper(*args, **kwargs):
            if getattr(args[0], attribute_name) is None:
                func(*args, **kwargs)
                return None
            else:
                return None

        return wrapper

    return inner
