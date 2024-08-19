import pandas as pd
from unidecode import unidecode

from source.data_merger.disaster_merger_3 import DisasterLinker
from source.scraping.event import Event
from typing import Callable


def culler(df: pd.DataFrame) -> pd.DataFrame:
    # For now, the culler does nothing as we don't know what parameter to base the culling on
    return df


def generate_search_query(self: Event) -> str:
    numero_mes = self.start_time.month
    mes = ["enero", "febrero", "marzo", "abril", "mayo", "junio",
           "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"][numero_mes - 1]
    human_readable_query = unidecode(
        f"{self.theme} {self.location} {mes}"
    )
    return human_readable_query.replace(' ', '+')


def disaster_to_dict_factory(event_list: list[Event]) -> Callable[[DisasterLinker], dict | list[dict]]:
    def disaster_to_dict(disaster: DisasterLinker) -> list[dict]:
        # Define how to generate a row for the result dataframe from a disaster instace (a cluster of events)
        event_groups = {}
        for event in event_list:
            if event.df_index not in disaster.indexes:
                continue
            if event.theme not in event_groups.keys():
                event_groups[event.theme] = []
            event_groups[event.theme] += [event]
        dict_list = []
        for theme in event_groups.keys():
            dict_list.append(Event.combine_events(event_groups[theme]))
        return dict_list
    return disaster_to_dict
