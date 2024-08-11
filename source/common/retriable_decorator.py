def retriable(max_n_of_tries: int):
    """
    Decorator. Will call the function it decorates up to n times, retrying if the function raises any exception
    After n tries, if the function excepts the exception will be raised anyway.

    :param max_n_of_tries: The maximum njumber of retries allowed
    """
    def inner(func):
        def wrapper(*args, **kwargs):
            total_tries = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    total_tries += 1
                    if total_tries >= max_n_of_tries:
                        raise e
        return wrapper
    return inner

