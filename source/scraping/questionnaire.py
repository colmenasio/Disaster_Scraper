import json
from typing import Iterator


class Question:
    id_counter = 0

    def __init__(self, question_arg: str, weight_arg: int):
        self.question = question_arg
        self.weight = weight_arg
        self.id = Question.id_counter
        Question.id_counter += 1

    def __str__(self):
        return self.question

    def __repr__(self):
        return f"Question {self.id} (weight: {self.weight}): {self.question}"


class Questionnaire:
    QUESTIONS_PATH = "../data/gpt_parser_data/questions.json"

    with open(QUESTIONS_PATH) as fstream:
        raw_questions = json.load(fstream)
        questions, sectors_descriptions = {}, {}
    for key in raw_questions.keys():
        questions[key] = [Question(x["q"], x["w"]) for x in raw_questions[key]["questions"]]
        sectors_descriptions[key] = raw_questions[key]["description"]
    del raw_questions

    def __init__(self, sectors_arg: list[str]):
        self.sectors = sectors_arg

    def __iter__(self) -> Iterator[Question]:
        for sector in self.sectors:
            sector_questions = self.questions.get(sector)
            if sector_questions is None:
                continue
            for sector_question in sector_questions:
                yield sector_question

    @classmethod
    def get_sector_list(cls) -> list[str]:
        return list(cls.questions.keys())

    @classmethod
    def get_sector_descriptions(cls):
        return cls.sectors_descriptions

    @classmethod
    def get_question_id_dict(cls) -> dict[int, str]:
        question_dict = {}
        for sector_questions in cls.questions.values():
            for q in sector_questions:
                question_dict[q.id] = q.question
        return question_dict


if __name__ == '__main__':
    print(Questionnaire.questions)
    questionnaire_test = Questionnaire(["Agua potable", "Transporte"])
    for question in questionnaire_test:
        print(question.id, question.question)
