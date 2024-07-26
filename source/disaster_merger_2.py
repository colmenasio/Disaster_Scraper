from __future__ import annotations
import pandas as pd


class Disaster:
    """Each instance represents a cluster of rows in the 'expedients' dataframe
    that are considered to be the same disaster"""

    expedients = pd.read_csv("../input-output/merged_expedients_1.csv")
    adjacencies = pd.read_csv("../data/provinces_adjacency/adjacency_table.csv")

    def __init__(self, indexes: list[int]):
        self.indexes = indexes

    def build_initial_disaster_pool(self) -> list[Disaster]:
        """Factory method creating an initial list of disaster instances by considering each row in 'expedients'
        a different disaster"""
        n_of_rows = self.expedients.shape[0]
        return [Disaster([n]) for n in range(n_of_rows)]

    def is_compatible_with(self, other) -> bool:
        """Check if two instances represent the same disaster in a different place"""
        raise NotImplemented

    def merge_with(self, other: Disaster) -> Disaster:
        """Merges two instances that represent the same disaster.
        :except Nothing: Even though this method doesn't check, it is expected that *self* and *other* are compatible
        (in other words, 'self.is_compatible_with(other)' must return True)"""
        return Disaster(self.indexes+other.indexes)

    @staticmethod
    def collapse_disaster_list(disasters: list[Disaster]) -> list[Disaster]:
        """Takes a list of Disaster instances, groups them using the adjacency table and returns a new list
        of instances"""
        new_list = []
        while len(disasters) > 0:
            # Take a disaster from the list and check if it can be combined with any other disaster
            current_disaster = disasters.pop()
            compatible_disaster_index = current_disaster.find_compatible_disasters(disasters)

            if compatible_disaster_index >= 0:
                # A compatible disaster was found. Merge the two disasters and add them back to the disaster pool
                other_disaster = disasters.pop(compatible_disaster_index)
                disasters.append(current_disaster.merge_with(other_disaster))
            else:
                # No compatible disaster was found. Add the current disaster to the final list
                new_list.append(current_disaster)
        return new_list

    def find_compatible_disasters(self, other_disasters: list[Disaster]) -> int:
        """
        Returns the index of the first disaster in the provided list that can be merged with *self*
        :return: -1 if *self* cannot be merged with any disaster in the list
        """
        for other_disaster_index, other_disaster in enumerate(other_disasters):
            if self.is_compatible_with(other_disaster):
                return other_disaster_index
        # If it cannot be combined with any other disaster, return -1
        return -1
