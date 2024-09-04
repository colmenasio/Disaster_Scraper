from __future__ import annotations

from typing import Generator

import pandas as pd

from common.merge_dictionaries import merge_dicts


class SummarizedEvent:
    """mostly a dict wrapper so that it `kinda` behaves like a Event instance
    and can reuse most of the merging code lmao"""

    ARTICLES = None

    def __init__(self, data_arg: dict):
        """
        keys:
        theme: str
        location: str
        start_time: datetime64
        end_time: datetime64
        df_index: int
        sources: list[str]
        affected_sectors: list[str]
        severity_ratio: dict[str, float]
        answer_ratio: dict[int, float]
        """
        if list(data_arg.keys()) == ["theme", "start_time", "end_time", "sources", "location", "df_index"]:
            self.version = "reference"
        else:
            raise ValueError("Summarized event in the wrong format")
        self.data = data_arg

    def __getattr__(self, item):
        if item in self.data:
            return self.data[item]
        else:
            raise AttributeError("'SummarizedEvent' object has no attribute 'event_id'")

    def make_article_generator(self) -> Generator[dict, None, None]:
        if self.ARTICLES is None:
            raise ValueError("Articles were not initialized "
                             "(make sure to run the initialize_referenced_articles class method)")
        for a_id in self.sources:
            yield dict(self.ARTICLES.loc[a_id])

    @classmethod
    def initialize_referenced_articles(cls, filepath):
        cls.ARTICLES = pd.read_csv(filepath,
                                   converters={key: lambda x: eval(x) for key in ['sectors', 'severity', 'answers']}
                                   ).set_index("id")

    @staticmethod
    def combine_events(events: list[SummarizedEvent]) -> dict:
        if len(events) == 0:
            raise ValueError("No articles were provided")
        info = {}
        theme = events[0].theme
        if any((e.theme != theme for e in events)):
            raise ValueError("Inconsistent themes")
        info["theme"] = theme
        info["locations"] = list({e.location for e in events})
        info["start_time"] = min([e.start_time for e in events])
        info["end_time"] = max([e.end_time for e in events])
        info["sources"] = list({source for e in events for source in e.sources})
        info["df_indexes"] = list({int(e.df_index) for e in events})

        def min_mean_max(l: list) -> tuple:
            if len(l) == 0:
                raise ValueError("The current summarized_event has no values")
            return max(l), sum(l) / len(l), min(l)

        info["affected_sectors"] = list({s for e in events for a in e.make_article_generator() for s in a["sectors"]})
        severities_generator = (a["severity"] for e in events for a in e.make_article_generator())
        info["severity_ratio"] = merge_dicts(severities_generator, min_mean_max)
        answers_generator = (a["answers"] for e in events for a in e.make_article_generator())
        info["answer_ratio"] = merge_dicts(answers_generator, min_mean_max)
        return info

    def __repr__(self):
        return f"<__main__.Event object: {self.theme}, {self.location}, {self.start_time}>"
