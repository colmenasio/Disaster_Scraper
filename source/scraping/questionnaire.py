import json
from typing import Iterator

class Question:
    id_counter = 0

    def __init__(self, question_arg: str):
        self.question = question_arg
        self.id = Question.id_counter
        Question.id_counter += 1

    def __str__(self):
        return self.question

    def __repr__(self):
        return f"Question {self.id}: {self.question}"


class Questionnaire:
    QUESTIONS_PATH = "../../data/gpt_parser_data/questions.json"
    DEFINITIONS_PATH = "../../data/gpt_parser_data/definiciones.json"

    with open(QUESTIONS_PATH) as fstream:
        questions = json.load(fstream)
    for key in questions.keys():
        questions[key] = [Question(x) for x in questions[key]]

    def __init__(self, sectors_arg: list[str]):
        self.sectors = sectors_arg

    def __iter__(self) -> Iterator[Question]:
        for sector in self.sectors:
            sector_questions = self.questions.get(sector)
            if sector_questions is None:
                continue
            for sector_question in sector_questions:
                yield sector_question


if __name__ == '__main__':
    print(Questionnaire.questions)
    questionnaire_test = Questionnaire(["Agua potable", "Transporte"])
    for question in questionnaire_test:
        print(question.id, question.question)
