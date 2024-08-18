from typing import Any, Callable, Iterable, Generator, TypeVar

K = TypeVar("K")
V = TypeVar("V")
R = TypeVar("R")


def merge_dicts(dicts: list[dict[K, V]] | Iterable[dict[K, V]] | Generator[dict[K, V], None, None],
                merging_operator: Callable[[list[V]], R]
                ) -> dict[K, R]:
    uncombined_dict = {}
    for d in dicts:
        for key in d.keys():
            if key not in uncombined_dict.keys():
                uncombined_dict[key] = []
            uncombined_dict[key] = uncombined_dict[key] + [d[key]]
    combined_dict = {}
    for sector in uncombined_dict.keys():
        combined_dict[sector] = merging_operator(uncombined_dict[sector])
    return combined_dict
