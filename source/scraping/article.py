from __future__ import annotations

from typing import Callable

import requests
from numpy import datetime64
from googlesearch import search as g_search
from bs4 import BeautifulSoup
import openai
import json

from scraping.questionnaire import Questionnaire
from common.retriable_decorator import retriable
from common.merge_dictionaries import merge_dicts
from common.idempotent_attribute_setter import idempotent_attribute_setter
from common.exceptions import InformationFetchingError, IPBanError


class Article:
    OPEN_AI_KEY_PATH = "../config/credentials/OPENAI_API_KEY.json"
    CONFIG_PATH = "../config/article/article.json"

    try:
        with open(OPEN_AI_KEY_PATH, "r", encoding="utf-8") as fstream:
            openai_api_key = json.load(fstream)["OPENAI_API_KEY"]
            openai_client = openai.OpenAI(api_key=openai_api_key)
    except FileNotFoundError as e:
        raise FileNotFoundError(f"{OPEN_AI_KEY_PATH} not found")
    except KeyError as e:
        raise KeyError(f"'OPENAI_API_KEY' attribute not found in {OPEN_AI_KEY_PATH}")

    try:
        with open(CONFIG_PATH, "r") as fstream:
            CONFIG = json.load(fstream)
    except FileNotFoundError as e:
        raise FileNotFoundError(f"{CONFIG_PATH} not found")

    CACHE = dict()

    def __new__(cls,
                title_arg: str,
                source_url_arg: str,
                source_name_arg: str,
                date_arg: datetime64,
                do_processing_on_instanciation: bool = True,
                link_arg: str = None
                ):
        # Check if an instance with the same id already exists in the cache
        if title_arg in cls.CACHE:
            self = cls.CACHE[title_arg]
            return self
        else:
            # Create a new instance
            self = super().__new__(cls)
            cls.CACHE[title_arg] = self
            return self

    def __init__(self,
                 title_arg: str,
                 source_url_arg: str,
                 source_name_arg: str,
                 date_arg: datetime64,
                 do_processing_on_instanciation: bool = True,
                 link_arg: str = None):
        """
        Each `Article` instance represent a single news article with
        its respective information and its answers to the questions provided

        :param do_processing_on_instanciation:
            By default, all the scraping, classifying and answering is done on instanciation.
            This can be a hinderance if pre-processing is to be done
            (like filtering by date and such), causing an unecessary workload.
            This automatic processing can be manually toggled off by setting `do_processing` to False when instanciating,
            and then processed by calling `self.do_procesing`"""
        if "title" in self.__dict__:
            # Already has been initialized so no need to initialize again
            return
        self.title = title_arg
        self.source_url = source_url_arg
        self.source_name = source_name_arg
        self.date = date_arg
        self.sucessfully_built = False
        self.link = link_arg
        self.contents = None
        self.sectors = None
        self.answers = None
        self.severity = None

        if do_processing_on_instanciation:
            self.process_article()

    @idempotent_attribute_setter("sucessfully_built")
    def process_article(self):
        """Idempotent function for scraping, classifying and answering the questions about the news article"""
        try:
            print(f"    Processing article: {self}")
            self.obtain_link_by_google()
            self.obtain_contents_from_link()
            self.classify_into_sectors()
            self.obtain_severities_questions()
            self.severity = Questionnaire(self.sectors).get_severity_score_by_sector(self.answers)
            self.sucessfully_built = True
        except InformationFetchingError as e:
            self.sucessfully_built = False
            print(f"Error building {self.title}; Message: {e.message}; Exception ocurred: {e.inner_exception}")

    @idempotent_attribute_setter("link")
    def obtain_link_by_google(self) -> None:
        # TODO Refine this search query
        # TODO MAKE PATCHES TO SAFELY STOP THE SCRIP ON GOOGLE IP BAN
        try:
            query = f'"{self.title}, {self.source_name}"'
            enlaces = list(g_search(query, num_results=1))
        except requests.exceptions.RequestException as e:
            if e.response is not None and e.response.status_code == 429:
                raise IPBanError
            else:
                raise InformationFetchingError(
                    inner_exception=e,
                    message="Error when calling the google api")

        if len(enlaces) == 0:
            raise InformationFetchingError(message="Article could not be found by a google search of its title")
        self.link = enlaces[0]

    @idempotent_attribute_setter("contents")
    def obtain_contents_from_link(self) -> None:
        # TODO Implement performance upgrades
        try:
            response = requests.get(self.link)
            page = BeautifulSoup(response.content, 'html.parser')
        except requests.exceptions.RequestException as e:
            raise InformationFetchingError(inner_exception=e,
                                           message=f"Could not get a response from the url: {self.link}")

        # Extraer título y párrafos como ejemplo de contenido
        # TODO extracting all the paragraphs MAY be just a little unprecise, fetching paragraphs that dont
        #  necesarilly belong on the new and could affect the OpenAI performace
        parrafos = page.find_all('p')
        if len(parrafos) == 0:
            raise InformationFetchingError(message="No <p> tags were found in the article")
        contenido = ' '.join([p.text for p in parrafos])
        self.contents = contenido
        return None

    @idempotent_attribute_setter("sectors")
    def classify_into_sectors(self) -> None:
        affected_sectors = []
        for sector in Questionnaire.get_sector_list():
            sector_description = Questionnaire.get_sector_descriptions()[sector]
            question = (f"En la noticia, ¿se menciona cualquier dato relacionado con el sector {sector}?\n"
                        f"En otras palabras, {sector_description}")
            if self.ask_bool_question(question) is True:
                affected_sectors.append(sector)
        self.sectors = affected_sectors

    @idempotent_attribute_setter("answers")
    def obtain_severities_questions(self) -> None:
        questionnaire = Questionnaire(self.sectors)
        answers = {}
        for q in questionnaire:
            a = self.answer_severity_scale_question(q.question)
            if a is None:
                continue
            else:
                answers[q.id] = a
        self.answers = answers

    @retriable(CONFIG["max_openai_call_tries"])
    def ask_bool_question(self, bool_question: str) -> bool:
        sys_prompt = ("Eres una herramienta de extraccion de datos.\n"
                      "A continuacion, se te provera un fragmento de un articulo de un noticiario. "
                      "Immediatamente despues, se te proporcionara una pregunta de si/no que deberás "
                      "responder en base a la informacion presente en la noticia.\n"
                      "A la hora de responder, sigue las siguientes pautas:\n"
                      "- Se lo mas breve y conciso posible. Evita redundancias como repetir la pregunta\n"
                      "- Responde unicamente o bien `si` o bien `no`. "
                      "- En caso de no haber suficiente informacion para decidir entre `si` y `no`, responde `no`")
        content_prompt = (f"Noticia:\n"
                          f"`{self.title}\n{self.contents}`\n"
                          f"Pregunta: {bool_question}")

        try:
            response = self.get_completion(
                system_prompt=sys_prompt,
                content_prompt=content_prompt
            )
        except openai.APIError as e:
            raise InformationFetchingError(inner_exception=e, message="Error ocurred when calling OpenAI completion")

        # Limpiar el contenido del response si es necesario
        if response.lower().strip(".") == "si":
            return True
        elif response.lower().strip(".") == "no":
            return False
        else:
            raise InformationFetchingError(
                message="Error ocurred when parsing OpenAI response (in `article.ask_bool_question`)"
            )

    @retriable(CONFIG["max_openai_call_tries"])
    def answer_single_question(self, question: str) -> str | None:
        sys_prompt = ("Eres una herramienta de extraccion de datos.\n"
                      "A continuacion, se te provera un fragmento de un articulo de un noticiario. "
                      "Immediatamente despues, se te proporcionara una pregunta que deberás "
                      "responder en base a la informacion presente en la noticia.\n"
                      "A la hora de responder, sigue las siguientes pautas:\n"
                      "- Se lo mas breve y conciso posible. Evita redundancias como repetir la pregunta\n"
                      "- En caso de que el articulo no contenga la informacion "
                      "necesaria para responder con cierto grado de confianza, responde solamente `null`, sin comillas")
        content_prompt = (f"Noticia:\n"
                          f"`{self.title}\n{self.contents}`\n"
                          f"Pregunta: {question}")

        try:
            response = self.get_completion(
                system_prompt=sys_prompt,
                content_prompt=content_prompt
            )
        except openai.APIError as e:
            raise InformationFetchingError(inner_exception=e, message="Error ocurred when calling OpenAI completion")

        # Limpiar el contenido del response si es necesario
        # TODO Add type casting
        if response.lower() == "null":
            return None
        else:
            return response

    @retriable(CONFIG["max_openai_call_tries"])
    def answer_severity_scale_question(self, question: str) -> float | None:
        """
        Returns a float from 0 to 1 indicating the severity of the impact of self in a sector specified by the question
        parameter. In case no answer can be provided, None is returned

        I'm in too much of a hurry to parametrize it so for now it's hard coded to work on a scale of 1-5"""

        sys_prompt = ("Eres una herramienta de extraccion de datos.\n"
                      "A continuacion, se te provera un fragmento de un articulo de un noticiario "
                      "hablando acerca de un desastre natural o similar.\n"
                      "Immediatamente despues, se te proporcionara una pregunta acerca la gravedad con la que ha "
                      "afectado dicho desastre a un determinado sector."
                      "Debes responder la pregunta en una escala del 1 al 5 en base como de severamente se ha visto "
                      "afectado el sector descrito en la pregunta.\n"
                      "En cuanto a la escala en la que has de responder:\n"
                      "- Un '1' equivale a: "
                      "'El sector apenas se ha visto afectado\\Las afecciones han sido leves'\n"
                      "- Un '5' equivale a: "
                      "'El sector se ha visto extremadamente afectado\\Las afecciones han sido muy graves'\n"
                      "En el caso de que no se mencionen afecciones al sector en question, responde '0'\n"
                      "A la hora de responder, sigue las siguientes pautas:\n"
                      "- Responde únicamente con un numero entero entre el 0 y el 5, ambos incluidos\n"
                      "- Recuerda que en caso de no haber mención a afecciones al sector en cuestion, "
                      "debes responder '0'")
        content_prompt = (f"Noticia:\n"
                          f"`{self.title}\n{self.contents}`\n"
                          f"Pregunta: {question}")

        try:
            response = self.get_completion(
                system_prompt=sys_prompt,
                content_prompt=content_prompt
            )
        except openai.APIError as e:
            raise InformationFetchingError(inner_exception=e, message="Error ocurred when calling OpenAI completion")

        # Type cast the response
        try:
            result = int(response)
            if result == 0:
                return None
            else:
                return (result - 1) / 4
            # return None if result == 0 else (result-1)/4
        except ValueError as e:
            raise InformationFetchingError(inner_exception=e,
                                           message="OpenAI response could not be parsed")

    @classmethod
    def get_completion(cls, content_prompt: str, system_prompt: str = "", model="gpt-4o"):
        messages = []
        if system_prompt != "":
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": content_prompt})
        response = cls.openai_client.chat.completions.create(
            model=cls.CONFIG["GPT_VERSION"],
            messages=messages
        )
        return response.choices[0].message.content

    @classmethod
    def get_completion_obsolete(cls, content_prompt: str, system_prompt: str = "", model="gpt-4o"):
        messages = []
        if system_prompt != "":
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": content_prompt})
        response = openai.ChatCompletion.create(
            model=model,
            messages=messages,
            temperature=0,  # this is the degree of randomness of the model's output
        )
        return response.choices[0].message["content"]

    def format_title_body(self) -> str:
        return f"Title: {self.title}\n\nContent:{self.contents}"

    @staticmethod
    def get_combined_severity(articles: list[Article],
                              combination_operation: Callable[[list[float]], float] = lambda x: sum(x) / len(
                                  x)) -> dict:

        """
        DEPRECATED

        The resulting severity is the mean of the severities by default.
        This can be changed by providing a custom combination operation"""
        dict_generator = (a.severity for a in articles)
        return merge_dicts(dict_generator, combination_operation)

    @staticmethod
    def get_answers_score_ratio(articles: list[Article]) -> dict:
        """DEPRECATED"""

        def combination_operation(list_arg: list[bool]) -> float:
            return sum(list_arg) / len(list_arg)

        dict_generator = (a.answers for a in articles)
        return merge_dicts(dict_generator, combination_operation)

    def __repr__(self):
        return f"<__main__.Article object: {self.title}, {self.source_url}, {self.date}>"
