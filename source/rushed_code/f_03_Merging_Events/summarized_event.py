from __future__ import annotations

from common.merge_dictionaries import merge_dicts


class SummarizedEvent:
    """mostly a dict wrapper so that it `kinda` behaves like a Event instance
    and can reuse most of the merging code lmao"""

    def __init__(self, data_arg: dict):
        """
        keys:
        theme: str
        location: str
        start_time: datetime64
        end_time: datetime64
        affected_sectors: list[str]
        df_index: int
        severity_ratio: dict[str, float]
        answer_ratio: dict[int, float]
        """
        self.data = data_arg

    def __getattr__(self, item):
        if item in self.data:
            return self.data[item]
        else:
            raise AttributeError("'SummarizedEvent' object has no attribute 'event_id'")

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
        info["affected_sectors"] = list({s for e in events for s in e.affected_sectors})
        info["event_ids"] = list({e.df_index for e in events})
        severities_generator = (e.severity_ratio for e in events)
        info["severity_ratio"] = merge_dicts(severities_generator, lambda x: sum(x) / len(x))
        answers_generator = (e.answer_ratio for e in events)
        info["answer_ratio"] = merge_dicts(answers_generator, lambda x: sum(x) / len(x))
        return info
