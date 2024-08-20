from source.scraping.event import Event
from source.scraping.article import Article

from unidecode import unidecode

import time

START_TIME = time.time()
print("Imports/Initialization: Done\nStarting unit test")
Event.INPUT_PATH = "../utility/testfiles/sample_merged_1.csv"
Event.MAX_NEWS_PER_EVENT = 6
event_list = Event.from_csv()
NUMBER_OF_EVENTS = len(event_list)


def generate_query(self: Event) -> str:
    numero_mes = self.start_time.month
    mes = ["enero", "febrero", "marzo", "abril", "mayo", "junio",
           "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"][numero_mes - 1]
    human_readable_query = unidecode(
        f"{self.theme} {self.location} {mes}"
    )
    return human_readable_query.replace(' ', '+')


Event.get_related_news_concurrent(event_list, generate_query)
event_list = Event.filter_out_irrelevant_events(event_list)
print(len(event_list))
if len(event_list) == 0:
    print("No articles were relevant")
else:
    print(Article.get_combined_severity(event_list[0].related_articles))
    print(Article.get_answers_true_ratio(event_list[0].related_articles))
END_TIME = time.time()
ELAPSED_TIME = END_TIME-START_TIME
print(f"DEBUG INFO\nTOOK {ELAPSED_TIME} TO PROCESS {NUMBER_OF_EVENTS} EVENTS")