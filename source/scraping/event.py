from __future__ import annotations

from bs4 import BeautifulSoup as soup
from urllib.request import urlopen
import pandas as pd
import numpy as np

from source.scraping.article import Article


class Event:
    """Each instance represents a single event"""

    INPUT_PATH = ""
    OUTPUT_PATH = ""

    def __init__(self, theme_arg: str, location_arg: str, start_time: np.datetime64, end_time=np.datetime64):
        self.theme = theme_arg
        self.location = location_arg
        self.start_time = start_time
        self.end_time = end_time

    def get_related_news(self, amount=30) -> [Article]:
        # TODO modify to integrate the filtering in the fetching process
        try:
            # Convertir el tema en un formato adecuado para la URL
            formatted_theme = self.theme.replace(' ', '+')
            url_google_news = f'https://news.google.com/rss/search?q={formatted_theme}&hl=es-419&gl=US&ceid=US:es-419'

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
            for item in items[:amount]:
                articles.append(Article(item.title.text, item.link.text, item.pubDate.text))

            return articles
        except Exception as e:
            # TODO ADD MORE DEBUG INFO HERE
            print("Error al obtener noticias:", str(e))
            return []

    @classmethod
    def from_csv(cls) -> list[Event]:
        """The input path is defined as a class attribute"""
        df = pd.read_csv(cls.INPUT_PATH, parse_dates=["date"])
        theme_dictionary = {
            "CAUSAS NATURALES/INUNDACIÓN EXTRAORDINARIA": "",
            "etc": ""
        }
        raise NotImplementedError("theme dictionary infinished")
        events = []
        for index, row in df.iterrows():
            events.append(Event(
                theme_dictionary.get(row["disaster"]),
                row["province"],
                row["date"],
                row["date"] + pd.Timedelta(days=row["duration"]))
            )
        return events

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
