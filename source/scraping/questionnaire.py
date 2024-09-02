import json
import numbers
from typing import Iterator


class Question:
    id_counter = 0

    def __init__(self, sector_arg: str, question_arg: str, weight_arg: int):
        self.sector = sector_arg
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
        questions, sectors_descriptions, sector_weight_sum = {}, {}, {}
    for sector in raw_questions.keys():
        curr_questions = []
        for q in raw_questions[sector]["questions"]:
            curr_questions.append(Question(sector, q["q"], q["w"]))
        questions[sector] = curr_questions
        sectors_descriptions[sector] = raw_questions[sector]["description"]
        sector_weight_sum[sector] = sum([q.weight for q in questions.get(sector)])
    del raw_questions, curr_questions

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
    def get_question_id_dict(cls) -> dict[int, (str, int, str)]:
        """Each value in the dict corresponds, respectively,
        to the question itself, the weight of said question and the sector the question belongs to"""
        question_dict = {}
        for sector_questions in cls.questions.values():
            for q in sector_questions:
                question_dict[q.id] = (q.question, q.weight, q.sector)
        return question_dict

    def get_severity_score_by_sector(self, answers: dict[int, bool]) -> dict[str, float]:
        """Calculates the score ratio of a set of answers for each sector that self.sectors contains
        (for example, if in "sectorA" there's 3 questions with a weight of 5 each, and in answers the values
        whose keys correspond to the id of those 3 questions are `True, True, False`,
        then the return value would be 10/15)
        :param self: An instance that determines which sectors are considered
        :param answers: A dictionary of answers to questuons in which keys are the id of the question being answered
            missing answers are considered as false
        :return: A dictionary in which each sector is a sector their value represents their score over 1"""
        # Initialize sector scores
        actual_scores = {}
        for sector in self.sectors:
            actual_scores[sector] = 0

        # Iterate over questions
        for question in self:
            answer = answers.get(question.id)
            # Either an int, a float or a bool
            if not isinstance(answer, (numbers.Integral, numbers.Real)):
                continue
            actual_scores[question.sector] += question.weight * answer

        # Calculate the final score
        ratios_dict = {}
        for sector in self.sectors:
            if self.sector_weight_sum[sector] == 0:
                continue
            ratios_dict[sector] = actual_scores[sector] / self.sector_weight_sum[sector]
        return ratios_dict


if __name__ == '__main__':
    print(Questionnaire.questions)
    questionnaire_test = Questionnaire(["Agua y Alimentos", "Transporte"])
    answers_test = {14: True, 1: False, 16: True, 15: False, 7: True}
    for q in questionnaire_test:
        print(q.id, q.question)
    print(questionnaire_test.get_severity_score_by_sector(answers_test))
