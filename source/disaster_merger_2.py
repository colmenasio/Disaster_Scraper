from __future__ import annotations
import pandas as pd
import numpy as np
from json import load as jsonload


class Disaster:
    """Each instance represents a cluster of rows in the 'expedients' dataframe
    that are considered to be the same disaster"""

    expedients = pd.read_csv("../input-output/merged_expedients_1.csv")
    expedients['date'] = pd.to_datetime(expedients['date'], errors='raise')
    adjacencies = pd.read_csv("../data/provinces_adjacency/adjacency_table.csv")

    with open("../config/disaster_merger_2/config.json") as fstream:
        CONFIG = jsonload(fstream)

    def __init__(self, indexes: list[int]):
        self.indexes = indexes

    def build_initial_disaster_pool(self) -> list[Disaster]:
        """Factory method creating an initial list of disaster instances by considering each row in 'expedients'
        a different disaster"""
        n_of_rows = self.expedients.shape[0]
        return [Disaster([n]) for n in range(n_of_rows)]

    def is_compatible_with(self, other: Disaster) -> bool:
        """Check if two instances represent the same disaster in a different place

        For 2 disasters to be considered compatible, all the following conditions must be met:

        - The disaster type must be the same

        - The time elapsed between the two disasters must be less than the speficied in the config

        - At least 2 of the provinces in the disasters must be adjacent
        """
        # Check the disasters are of the same type
        if self.get_disaster_type() != other.get_disaster_type():
            return False
        # Check the disasters overlap time-wise
        self_duration = self.get_total_duration()
        self_duration[1] = self_duration[1] + pd.Timedelta(days=self.CONFIG["days_leniency"])
        other_duration = other.get_total_duration()
        other_duration[1] = other_duration[1] + pd.Timedelta(days=other.CONFIG["days_leniency"])
        if other_duration[0] > self_duration[1] or self_duration[0] > other_duration[1]:
            return False
        # Check the disasters are adjacent space-wise
        if not self.is_adjacent_with(other):
            return False
        return True

    def get_disaster_type(self) -> str:
        """
        Get the disaster type of self
        Note that this method doesn't check whether the disaster types are consistent whithin the instance
        """
        return self.expedients.loc[self.indexes[0], "disaster"]

    def get_total_duration(self) -> [np.datetime64, np.datetime64]:
        """
        Get the total duration of a disaster by taking into account all the rows contained in the instance
        Note that this method doesn't check whether the dates are consistent whithin the instance

        :return: A list containing the start and end time of the Disaster
        """
        dates = self.expedients.loc[self.indexes, ["date", "duration"]]
        dates.rename(columns={"date": "start_date"}, inplace=True)
        dates["end_date"] = dates["start_date"]
        for index in dates.index:
            dates.loc[index, "end_date"] = (
                    dates.loc[index, "end_date"] + pd.Timedelta(days=dates.loc[index, "duration"]))
        return [min(dates["start_date"]), max(dates["end_date"])]

    def is_adjacent_with(self, other:Disaster) -> bool:
        """Check whether at least a pair of provinces covered by the disasters are adjacent"""
        raise NotImplemented

    def merge_with(self, other: Disaster) -> Disaster:
        """Merges two instances that represent the same disaster.
        :except Nothing: Even though this method doesn't check, it is expected that *self* and *other* are compatible
        (in other words, 'self.is_compatible_with(other)' must return True)"""
        return Disaster(self.indexes + other.indexes)

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


if __name__ == '__main__':
    test_instace = Disaster([1, 2])
    duration = test_instace.get_total_duration()
    type = test_instace.get_disaster_type()
    print(type)
