from __future__ import annotations

from datetime import datetime
from typing import Callable

from bs4 import BeautifulSoup as soup
from urllib.request import urlopen
import pandas as pd
import numpy as np
import json

from source.scraping.article import Article


class Event:
    """Each instance represents a single event"""

    INPUT_PATH = ""
    OUTPUT_PATH = ""

    MAX_NEWS_PER_EVENT = 30

    def __init__(self,
                 theme_arg: str,
                 location_arg: str,
                 start_time: np.datetime64,
                 end_time: np.datetime64,
                 df_index_arg: int = 0
                 ):
        self.df_index = df_index_arg
        self.theme = theme_arg
        self.location = location_arg
        self.start_time = start_time
        self.end_time = end_time
        self.related_articles = []

    def get_related_news(self,
                         query_generator: Callable[[Event], str],
                         do_date_filter: bool = True) -> None:
        """
        Fetches news related to the event. The amount of news fetched is a class attribute
        :param do_date_filter: filter news by date or ignore date
        :param query_generator: Its return type must be an ascii string with no whitespaces
            (use '+' instead, eg: 'this+is+an+example')
        """
        # TODO modify to integrate the filtering in the fetching process
        try:
            # Convertir el tema en un formato adecuado para la URL
            # formatted_theme = self.theme.replace(' ', '+')
            url_google_news = \
                f'https://news.google.com/rss/search?q={query_generator(self)}&hl=es&gl=US&ceid=US:es-419'

            # Conexión
            cliente = urlopen(url_google_news)
            contenido_xml = cliente.read()
            cliente.close()

            # Lectura en el formato XML
            pagina = soup(contenido_xml, 'xml')
            items = pagina.findAll('item')

            # Lista para almacenar las noticias
            articles = []

            # Iterar sobre las primeras noticias
            for item in items[:self.MAX_NEWS_PER_EVENT]:
                articles.append(Article(
                    title_arg=item.title.text,
                    source_url_arg=item.source.get("url"),
                    source_name_arg=item.source.text,
                    date_arg=np.datetime64(datetime.strptime(item.pubDate.text, "%a, %d %b %Y %H:%M:%S %Z")))
                )

            if do_date_filter:
                articles = self.filter_articles_by_date(articles)

            return articles
        except Exception as e:
            # TODO ADD MORE DEBUG INFO HERE
            print("Error al obtener noticias:")
            raise e

    def filter_articles_by_date(self, articles: list[Article]) -> list[Article]:
        def filter_key(article: Article) -> bool:
            return self.start_time <= article.date <= self.end_time

        filtered_news = list(filter(filter_key, articles))
        return filtered_news

    @staticmethod
    def filter_out_not_found_events(events_arg: list[Event]) -> list[Event]:
        """Filters out events for which related news have been searched and still have no related news"""
        return [event for event in events_arg if event.related_articles is not None and len(event.related_articles) > 0]

    @classmethod
    def from_csv(cls) -> list[Event]:
        """The input path is defined as a class attribute"""
        df = pd.read_csv(cls.INPUT_PATH, parse_dates=["date"])
        possible_themes_dictionary = {  # TODO REFINE THIS DICTIONARY
            "CAUSAS NATURALES/INUNDACIÓN EXTRAORDINARIA":
                ["Inundacion", "Luvias Torrenciales", "Desbordamiento de rìos"],
            "CAUSAS NATURALES/TEMPESTAD CICLÓNICA ATÍPICA":
                ["Tornado", "Huracan", "Tormenta"],
            "CAUSAS NATURALES/EMBATE DE MAR":
                ["Embate de mar", "Tsunami"],

        }
        events = []
        for index, row in df.iterrows():
            for theme in possible_themes_dictionary.get(row["disaster"]):
                events.append(Event(
                    theme,
                    row["location"],
                    row["date"],
                    row["date"] + pd.Timedelta(days=row["duration"]),
                    df_index_arg=int(index))
                )
        return events

    @classmethod
    def from_json(cls) -> list[Event]:
        try:
            with open(cls.INPUT_PATH, 'r', encoding='utf-8') as file:
                data = json.load(file)
        except Exception as e:
            print("Error al leer el archivo de eventos:")
            raise e
        events = []
        for event in data:
            events.append(Event(
                event["Idevento"],
                event["Location"],
                np.datetime64(event["FechaInicio"]),
                np.datetime64(event["FechaFin"]),
            ))
        return events

    def __repr__(self):
        return f"<__main__.Event object: {self.theme}, {self.location}, {self.start_time}>"

    @classmethod
    def extract_info_to_csv(cls, events: Event | [Event]) -> None:
        """The input path is defined as a class attribute"""
        # Sanitize input
        if isinstance(events, Event):
            events = [events]

        # Extract information about each event
        results_df = pd.DataFrame(columns=["1", "2"])
        # raise NotImplementedError("Not finished implementing information extraction")
        for curr_index, event in enumerate(events):
            results_df.loc[curr_index] = event.extract_info_event()
        results_df.to_csv(cls.OUTPUT_PATH)

    def extract_info_event(self) -> dict:
        """Extracts info from a single disaster"""
        raise NotImplementedError


if __name__ == '__main__':
    Event.INPUT_PATH = "og_proyect_files/Eventos.json"
    Event.MAX_NEWS_PER_EVENT = 6
    event_list = Event.from_json()

    def generate_query(self: Event) -> str:
        return self.theme.replace(' ', '+')

    for e in event_list:
        e.get_related_news(generate_query)
    # event_list = Event.filter_out_not_found_events(event_list)
    # articles = test_event.filter_articles_by_date(articles)
    print(event_list[1].related_articles)
    # print(news_articles[0].contents)
