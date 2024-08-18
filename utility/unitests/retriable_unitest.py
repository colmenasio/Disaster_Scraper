import random
from common.retriable_decorator import retriable


@retriable(max_n_of_tries=4)
def untrustworthy_function(message: str):
    r_value = random.randint(0, 10)
    if r_value > 2:
        raise ValueError("bad stuff happened")
    else:
        return f"{message}: {r_value}"


if __name__ == '__main__':
    for i in range(3):
        print(f"\nTEST NUMBER {i+1}")
        try:
            print(untrustworthy_function("R_VALUE IS"))
        except ValueError:
            print("exception was raised")