from source.common.merge_dictionaries import merge_dicts
import random

INDEXES = "abcdef"
NUMBER_DICTS = 5
KEYS_PER_DICT = 3
assert KEYS_PER_DICT <= len(INDEXES)

dictionaries = []
for _ in range(NUMBER_DICTS):
    curr_dict = {}
    curr_indexes = random.choices(INDEXES, k=KEYS_PER_DICT)
    for i in curr_indexes:
        v = random.randint(0, 10)
        curr_dict[i] = v
    dictionaries.append(curr_dict)


def merging_op(values):
    return sum(values)


result_l = merge_dicts(dictionaries, merging_op)

dict_generator = (x for x in dictionaries)
result_g = merge_dicts(dict_generator, merging_op)

print("Og dictionaries:")
print(dictionaries)
print("With List")
print(result_l)
print("With Generator")
print(result_g)
