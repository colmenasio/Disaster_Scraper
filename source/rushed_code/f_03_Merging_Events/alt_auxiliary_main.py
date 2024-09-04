from typing import Callable
from source.data_merger.disaster_merger_3 import DisasterLinker
from source.rushed_code.f_03_Merging_Events.summarized_event import SummarizedEvent


# Modified overrides of some of the auxiliary functions to account for the rushed changes

def disaster_to_dict_factory(event_list: list[SummarizedEvent], ) -> Callable[[DisasterLinker], dict | list[dict]]:
    def disaster_to_dict(disaster: DisasterLinker) -> list[dict]:
        # Define how to generate a row for the result dataframe from a disaster instace (a cluster of articles)
        print(f"Merging according to {disaster}")
        event_groups = {}
        for event in event_list:
            if event.df_index not in disaster.indexes:
                continue
            if event.theme not in event_groups.keys():
                event_groups[event.theme] = []
            event_groups[event.theme] += [event]
        dict_list = []
        for theme in event_groups.keys():
            dict_list.append(SummarizedEvent.combine_events(event_groups[theme]))
        return dict_list

    return disaster_to_dict
