from __future__ import annotations

import warnings
from datetime import datetime
from typing import Callable, Generator, Coroutine

from bs4 import BeautifulSoup as soup
from urllib.request import urlopen
import pandas as pd
import numpy as np
import json
import asyncio
import aiohttp

from source.scraping.article import Article
from source.common.merge_dictionaries import merge_dicts


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
                         do_date_filter: bool = True,
                         do_article_processing: bool = True) -> None:
        """
        Fetches news related to the event. The amount of news fetched is a class attribute
        :param do_date_filter: filter news by date or ignore date
        :param query_generator: Its return type must be an ascii string with no whitespaces
            (use '+' instead, eg: 'this+is+an+example')
        :param do_article_processing: Do info extraction automatically or not
        """
        # Run the fetching of the news
        asyncio.run(Event._async_get_related_news(self, query_generator, do_date_filter))

        # Scrape the article and answer the questions
        if do_article_processing:
            self.build_related_articles()

    @staticmethod
    def get_related_news_concurrent(events: [Event],
                                    query_generator: Callable[[Event], str],
                                    do_date_filter: bool = True,
                                    do_article_processing: bool = True) -> None:
        """Asyncronously fetch related news articles of an array of articles.

        Article processing is done sincronously as much as possible"""
        asyncio.run(Event._async_get_related_news(events, query_generator, do_date_filter))
        if do_article_processing:
            for event in events:
                event.build_related_articles()

    @staticmethod
    async def _fetch(url, session):
        async with session.get(url) as response:
            return await response.text()

    @staticmethod
    async def _async_get_related_news(events: Event | list[Event],
                                      query_generator: Callable[[Event], str],
                                      do_date_filter: bool = True
                                      ) -> None:
        if not isinstance(events, list):
            events = [events]
        async with aiohttp.ClientSession() as session:
            tasks = []
            for event in events:
                task = asyncio.create_task(event._async_get_related_news_task(session,
                                                                              query_generator,
                                                                              do_date_filter))
                tasks.append(task)
            await asyncio.gather(*tasks)

    async def _async_get_related_news_task(self,
                                           session: aiohttp.ClientSession,
                                           query_generator: Callable[[Event], str],
                                           do_date_filter: bool = True) -> None:
        """Asyncronous search of related articles
        :exception NotImplementedError: Asyncronous article processing is not yet supported.
            Setting do_article_processing to True will cause an exception to be raised"""
        # TODO modify to integrate the filtering in the fetching process
        try:
            # Convertir el tema en un formato adecuado para la URL
            # formatted_theme = self.theme.replace(' ', '+')
            url_google_news = \
                f'https://news.google.com/rss/search?q={query_generator(self)}&hl=es&gl=US&ceid=US:es-419'

            # Conexión
            contenido_xml = await Event._fetch(url_google_news, session)

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
            self.related_articles = articles

        except Exception as e:
            # TODO ADD MORE DEBUG INFO HERE
            print("Error al obtener noticias:")
            raise e

    def build_related_articles(self) -> None:
        """Builds related articles. Discards those news who couldn't be built"""
        if self.related_articles is None:
            return None
        for article in self.related_articles:
            article.process_article()
        self.related_articles = [article for article in self.related_articles if article.sucessfully_built]

    def filter_articles_by_date(self, articles: list[Article]) -> list[Article]:
        effective_end_time = self.end_time + pd.Timedelta(days=self.CONFIG["article_days_leniency"]).to_timedelta64()

        def filter_key(article: Article) -> bool:
            return self.start_time <= article.date <= effective_end_time

        filtered_news = list(filter(filter_key, articles))
        return filtered_news

    @staticmethod
    def filter_out_irrelevant_events(events_arg: list[Event]) -> list[Event]:
        # TODO filter out articles with no related article in any of their related articles
        """Filters out articles for which related news have been searched and still have no related news"""

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
                ["Embate de mar", "Tsunami"]
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

    @staticmethod
    def get_articles_iterable(events: list[Event]) -> Generator[Article, None, None]:
        """Generator that yields all the related articles in a list of articles"""
        for event in events:
            for article in event.related_articles:
                yield article

    @staticmethod
    def combine_events(events: list[Event]) -> dict:
        """Combines a list of articles merging their data into single dictionary of info
        The disasters to be merged must be consistent in theme
        :except ValueError: If the disasters cannot be merged because of an inconsistency
            or if no disasters are provided"""
        warnings.warn("event.combine_events() HAS NOT BEEN PROPPERLY TESTED")
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
        info["affected_sectors"] = list({a.sectors for a in Event.get_articles_iterable(events)})
        info["event_ids"] = list({e.df_index for e in events})
        severities_generator = (a.severity for a in Event.get_articles_iterable(events))
        info["severity_ratio"] = merge_dicts(severities_generator, lambda x: sum(x) / len(x))
        answers_generator = (a.answers for a in Event.get_articles_iterable(events))
        info["answer_ratio"] = merge_dicts(answers_generator, lambda x: sum(x) / len(x))
        return info
