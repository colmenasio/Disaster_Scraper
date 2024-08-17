from __future__ import annotations

from datetime import datetime
from typing import Callable

from bs4 import BeautifulSoup as soup
from urllib.request import urlopen
import pandas as pd
import numpy as np
import json

from source.scraping.article import Article

from unidecode import unidecode


class Event:
    """Each instance represents a single event"""

    INPUT_PATH = ""
    OUTPUT_PATH = ""

    CONFIG_PATH = "../config/event/event.json"
    with open(CONFIG_PATH) as fstream:
        CONFIG = json.load(fstream)

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
        self.related_articles = None

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
            client = urlopen(url_google_news)
            contenido_xml = client.read()
            client.close()

            # Lectura en el formato XML
            pagina = soup(contenido_xml, 'xml')
            items = pagina.findAll('item')

            # Lista para almacenar las noticias
            articles = []

            # Instanciate the Articles, toggling the automatic processing off
            for item in items[:self.CONFIG["max_articles_per_event"]]:
                articles.append(Article(
                    title_arg=item.title.text,
                    source_url_arg=item.source.get("url"),
                    source_name_arg=item.source.text,
                    date_arg=np.datetime64(datetime.strptime(item.pubDate.text, "%a, %d %b %Y %H:%M:%S %Z")),
                    do_processing_on_instanciation=False
                ))

            # Filter out irrelevent articles
            if do_date_filter:
                articles = self.filter_articles_by_date(articles)

            # Scrape the article and answer the questions
            for article in articles:
                article.process_article()
            self.related_articles = articles

        except Exception as e:
            # TODO ADD MORE DEBUG INFO HERE
            print("Error al obtener noticias:")
            raise e

    def filter_articles_by_date(self, articles: list[Article]) -> list[Article]:
        effective_end_time = self.end_time + pd.Timedelta(days=self.CONFIG["article_days_leniency"]).to_timedelta64()

        def filter_key(article: Article) -> bool:
            return self.start_time <= article.date <= effective_end_time

        filtered_news = list(filter(filter_key, articles))
        return filtered_news

    @staticmethod
    def filter_out_irrelevant_events(events_arg: list[Event]) -> list[Event]:
        # TODO filter out events with no related article in any of their related events
        """Filters out events for which related news have been searched and still have no related news"""
        def filter_is_event_relevant(e: Event) -> bool:
            if e.related_articles is None or len(e.related_articles) == 0:
                return False
            if all([a.answers is None or len(a.answers) == 0 for a in e.related_articles]):
                return False
            return True

        filtered_events = list(filter(filter_is_event_relevant, events_arg))
        return filtered_events

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
                    row["province"],
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

    @staticmethod
    def extract_info_events(events: list[Event],
                            query_generator: Callable[[Event], str],
                            do_relevancy_filter: bool = True) -> list[Event]:
        """In-place extraction of info from a list of disaster"""
        for event in events:
            event.get_related_news(query_generator)
        if do_relevancy_filter:
            events = Event.filter_out_irrelevant_events(events)
        return events


if __name__ == '__main__':
    print("Imports/Initialization: Done\nStarting unit test")
    Event.INPUT_PATH = "../utility/testfiles/sample_merged_1.csv"
    Event.MAX_NEWS_PER_EVENT = 6
    event_list = Event.from_csv()


    def generate_query(self: Event) -> str:
        numero_mes = self.start_time.month
        mes = ["enero", "febrero", "marzo", "abril", "mayo", "junio",
               "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"][numero_mes - 1]
        human_readable_query = unidecode(
            f"{self.theme} {self.location} {mes}"
        )
        return human_readable_query.replace(' ', '+')


    for evento in event_list:
        evento.get_related_news(generate_query)
    event_list = Event.filter_out_irrelevant_events(event_list)
    print(len(event_list))
