from source.common.idempotent_attribute_setter import idempotent_attribute_setter
from random import randint


class Example():
    def __init__(self):
        self.random_number = None

    @idempotent_attribute_setter("random_number")
    def produce_random_number(self):
        self.random_number = randint(0, 100)


a = Example()
print(a.random_number)
a.produce_random_number()
print(a.random_number)
a.produce_random_number()
print(a.random_number)
