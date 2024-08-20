from typing import Any, Callable


def idempotent_attribute_setter(attribute_name: str):
    """
    Decorator that only executes the callable if the attribute passed to it evaluates to false

    Useful for initialization of instances and avoinding redundant calls
    """

    def inner(func: Callable[[Any], None]):
        def wrapper(*args, **kwargs):
            if bool(getattr(args[0], attribute_name)):
                return None
            else:
                func(*args, **kwargs)
                return None

        return wrapper

    return inner
