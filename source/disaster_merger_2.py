from __future__ import annotations
import pandas as pd
import numpy as np
from json import load as jsonload


class Disaster:
    """Each instance represents a cluster of rows in the 'expedients' dataframe
    that are considered to be the same disaster"""

    expedients = pd.read_csv("../input-output/merged_expedients_1.csv")
    expedients['date'] = pd.to_datetime(expedients['date'], errors='raise')
    expedients.drop("Unnamed: 0", axis=1, inplace=True)
    adjacencies = pd.read_csv("../data/provinces_adjacency/adjacency_table.csv", index_col=0)

    with open("../config/disaster_merger_2/config.json") as fstream:
        CONFIG = jsonload(fstream)

    def __init__(self, indexes: list[int]):
        self.indexes = indexes
        self.total_duration = None
        self.disaster_type = None
        self.province_list = None

    @classmethod
    def build_initial_disaster_pool(cls) -> list[Disaster]:
        """Factory method creating an initial list of disaster instances by considering each row in 'expedients'
        a different disaster"""
        n_of_rows = cls.expedients.shape[0]
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
        other_duration = other.get_total_duration()
        if (other_duration[0] > self_duration[1] + pd.Timedelta(days=self.CONFIG["days_leniency"])
                or self_duration[0] > other_duration[1] + pd.Timedelta(days=self.CONFIG["days_leniency"])):
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
        if self.disaster_type is None:
            self.disaster_type = self.expedients.loc[self.indexes[0], "disaster"]
        return self.disaster_type

    def get_total_duration(self) -> [np.datetime64, np.datetime64]:
        """
        Get the total duration of a disaster by taking into account all the rows contained in the instance
        Note that this method doesn't check whether the dates are consistent whithin the instance

        :return: A list containing the start and end time of the Disaster
        """
        if self.total_duration is None:
            dates = self.expedients.loc[self.indexes, ["date", "duration"]]
            dates.rename(columns={"date": "start_date"}, inplace=True)
            dates["end_date"] = dates["start_date"]
            for index in dates.index:
                dates.loc[index, "end_date"] = (
                        dates.loc[index, "end_date"] + pd.Timedelta(days=dates.loc[index, "duration"]))
            self.total_duration = [min(dates["start_date"]), max(dates["end_date"])]
        return self.total_duration

    def get_province_list(self) -> list[str]:
        """Return a list of the provinces covered by a self"""
        if self.province_list is None:
            self.province_list = list(set(self.expedients.loc[self.indexes, "province"].values))
        return self.province_list

    def get_total_claims(self) -> int:
        total_claims = self.expedients.loc[self.indexes, "claims"].sum()
        return total_claims

    def get_total_cost(self) -> float:
        total_cost = self.expedients.loc[self.indexes, "total_cost"].sum()
        return total_cost

    def is_adjacent_with(self, other: Disaster) -> bool:
        """Check whether at least a pair of provinces covered by the disasters are adjacent"""
        self_provinces_list = self.get_province_list()
        other_provinces_list = other.get_province_list()
        return self.adjacencies.loc[self_provinces_list, other_provinces_list].values.any()

    def merge_with(self, other: Disaster) -> Disaster:
        """Merges two instances that represent the same disaster.
        :except Nothing: Even though this method doesn't check, it is expected that *self* and *other* are compatible
        (in other words, 'self.is_compatible_with(other)' must return True)"""
        # Join the disasters
        new_disaster = Disaster(self.indexes + other.indexes)
        # Most of this is just preformace improvements by taking advantage of the DP nature of the class
        # Dynamically generating the attributes from the new disaster instance
        new_disaster.disaster_type = self.get_disaster_type()
        new_disaster.province_list = list(set(self.get_province_list()).union(set(other.get_province_list())))
        self_durations = self.get_total_duration()
        other_durations = other.get_total_duration()
        new_disaster.total_duration = [min(self_durations[0], other_durations[0]),
                                       max(self_durations[1], other_durations[1])]
        return new_disaster

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
            if Disaster.CONFIG["debug_messages_on"]:
                print(f"Remaining Disasters: {len(disasters)}")
                print(f"Length of the new list: {len(new_list)}")
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

    def generate_data_dict(self) -> dict:
        """From a given instance, generates a dict of various values ready to be appended to a dataframe"""
        # total_cost,claims,losses_day
        data_dict = dict()
        # Calculate the date and the duration
        disaster_time_frame = self.get_total_duration()
        data_dict["date"] = disaster_time_frame[0]
        data_dict["duration"] = (disaster_time_frame[1] - disaster_time_frame[0]).days
        # Get the disaster type
        data_dict["disaster"] = self.get_disaster_type()
        # Get the list of provinces
        data_dict["provinces"] = self.get_province_list()
        # Get total amount of claims
        data_dict["claims"] = self.get_total_claims()
        # Get the total cost of the disaster
        data_dict["total_cost"] = self.get_total_cost()
        # Calculate the loss/day ratio
        data_dict["losses_per_day"] = data_dict.get("total_cost") / data_dict.get("duration")
        return data_dict

    @staticmethod
    def to_dataframe(instances: list[Disaster]) -> pd.DataFrame:
        disasters_df = pd.DataFrame(
            columns=["date", "duration", "disaster", "provinces", "claims", "total_cost", "losses_per_day"]
        )
        for df_index, disaster in enumerate(instances):
            data_dict = disaster.generate_data_dict()
            disasters_df.loc[df_index] = data_dict
        return disasters_df

    def __repr__(self):
        return f"|Indexes: {self.indexes}|"


if __name__ == '__main__':
    og_disasters = Disaster.build_initial_disaster_pool()
    collapsed_disasters = Disaster.collapse_disaster_list(og_disasters)
    print(collapsed_disasters)
    print(len(collapsed_disasters))
    final_df = Disaster.to_dataframe(collapsed_disasters)
    print(final_df)


    # TODO add method to convert list of Disaster instances into a dataframe
    #  (shouldnt be too dificult as most methods required for this are already implemented)
