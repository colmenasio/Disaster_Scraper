from numpy import datetime64
from selenium import webdriver
from bs4 import BeautifulSoup
import openai
import json


class Article:
    OPEN_AI_KEY_PATH = "../../config/credentials/OPENAI_API_KEY.json"
    DEFINITIONS_PATH = "../../data/gpt_parser_data/definiciones.json"
    QUESTIONS_PATH = "../../data/gpt_parser_data/questions.json"

    try:
        with open(OPEN_AI_KEY_PATH, "r", encoding="utf-8") as fstream:
            openai.api_key = json.load(fstream)["OPENAI_API_KEY"]
    except FileNotFoundError as e:
        raise FileNotFoundError(f"{OPEN_AI_KEY_PATH} not found")
    except KeyError as e:
        raise KeyError(f"'OPENAI_API_KEY' attribute not found in {OPEN_AI_KEY_PATH}")

    with open(DEFINITIONS_PATH, "r", encoding="utf-8") as fstream:
        DEFINITIONS = json.load(fstream)

    with open(QUESTIONS_PATH, "r", encoding="utf-8") as fstream:
        QUESTIONS = json.load(fstream)

    def __init__(self, title_arg: str, link_arg: str, date_arg: datetime64):
        self.state = 1
        self.title = title_arg
        self.link = link_arg
        self.date = date_arg
        self.contents = self.obtain_contents_from_link()
        self.sectores = self.obtain_sectors_affected()
        self.answers = self.obtain_answers_to_questions()

    def obtain_contents_from_link(self) -> str | None:
        # TODO Implement performance upgrades
        try:
            sel_options = webdriver.ChromeOptions()
            sel_options.add_argument("--headless")
            sel_driver = webdriver.Chrome(options=sel_options)
            sel_driver.get(self.link)
            sel_driver.implicitly_wait(4)
            response_content = sel_driver.page_source
            sel_driver.close()
            page = BeautifulSoup(response_content, 'html.parser')
            raise NotImplementedError("obtain_contents_from_link isnt fully implemented yet")

            # Extraer título y párrafos como ejemplo de contenido
            parrafos = page.find_all('p')
            if len(parrafos) == 0:
                return None
            # TODO extracting all the paragraphs MAY be just a little unprecise, fetching paragraphs that dont
            #  necesarilly belong on the new and could affect the OpenAI performace
            contenido = ' '.join([p.text for p in parrafos])
            return contenido
        except Exception as e:
            # TODO add better debug info
            return None

    def obtain_sectors_affected(self) -> str | None:
        # TODO THIS IS ABSOLUTELY PAINFUL TO LOOK AT. IMPROVING THIS IS A PRIORITY
        prompt = f"""
            Lee el archivo noticias.
            Lee el archivo "definiciones" para identificar los sectores y sus descripciones.
            En las noticias, detecta menciones relacionadas con los sectores sin buscar definiciones específicas.

            tu respuesta debe de ser la siguiente:
            Marca un 0 o 1 para cada sector según si se menciona o no algo relacionado en la noticia.
            Nombra los sectores
            Formatea la respuesta con un objeto JSON
            noticias: ```{self.title}; {self.contents}´´´
            definiciones: ```{self.DEFINITIONS}´´´
        """
        sectores_afectados = self.get_completion(prompt)

        prompt = f"""
            Devuelve una lista con los sectores que tienen un uno después del nombre del sector, en el archivo sectores afectados.
            Solo indica los nombres.
            Formatea la respuesta con un objeto JSON
            sectores afectados: ```{sectores_afectados}´´´ 
            """
        response = self.get_completion(prompt)
        return response

    def obtain_answers_to_questions(self) -> dict | None:
        # TODO THIS IS ABSOLUTELY PAINFUL TO LOOK AT. IMPROVING THIS IS A PRIORITY
        prompt = f"""
        Identifica los sectores afectados desde el archivo Sectores afectados. 
        Responde a las preguntas correspondientes de cada sector, formuladas en el archivo preguntas.
        Las respuestas deben ser específicas y claras, sin proporcionar un contexto general. Responde directamente a cada pregunta sin divagar.
        Asegúrate de que cada sector responda solo a las preguntas designadas para él.
        En el caso de transporte, identifica el tipo de transporte, y responde a las preguntas de dicho tipo.
        Las respuestas deben de ser breves. Como por ejemplo: si, no, grave... 
        La información necesaria para responder a las preguntas se encuentran en el archivo noticia.
        En el archivo noticia encontramos 10 diferentes noticias.
        Realiza lo anterior con todas las noticias.


        Formatea la respuesta con un objeto JSON en texto plano y sin cabecera json.
        El JSON debe contener Sector, Subsector, Preguntas y Respuestas.
        En Respuestas, NO vuelvas a escribir la pregunta, solo indica la respuesta.
        En el caso de que NO haya Subsector, no pongas null/none, simplemente indica:'' .
        Los subsectores se detectan en el archivo preguntas. Si hay varias descripciones dentro de uno de los sectores principales. 

        sectores afectados: ```{self.sectores}´´´
        preguntas: ```{self.QUESTIONS}´´´
        noticia: ```{self.title}; {self.contents}´´´
        """
        response = self.get_completion(prompt)

        # Limpiar el contenido del response si es necesario
        contenido_limpio = self.clean_json(response)

        # Verificar si el contenido limpio es un JSON válido
        try:
            return json.loads(contenido_limpio)
        except json.JSONDecodeError:
            print("El contenido no es un JSON válido después de la limpieza.")
            return None

    @staticmethod
    def clean_json(contenido):
        # Verificar y eliminar etiquetas ```json ... ```
        if contenido.startswith('```json') and contenido.endswith('```'):
            contenido = contenido[7:-3].strip()
        return contenido

    @classmethod
    def get_completion(cls, content_prompt: str, system_prompt: str = "", model="gpt-4o"):
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

    def __repr__(self):
        return f"<__main__.Article object: {self.title}, {self.link}, {self.date}>"
