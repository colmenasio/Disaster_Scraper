import json

# Independent script created as a command line tool to assist in creating and editing questions_OBSOLETE.json

QUESTIONS_PATH = "../data/gpt_parser_data/questions.json"


def open_questions_file() -> dict:
    global QUESTIONS_PATH
    while True:
        try:
            with open(QUESTIONS_PATH, "r") as fstream:
                return json.load(fstream)
        except FileNotFoundError:
            return {}


def add_new_sector(questionnaire: dict) -> dict:
    while True:
        sector_name = input("New Sector Name:\n")
        if sector_name in list(questionnaire.keys()):
            print("Sector already exists")
            continue
        sector_description = input("Question that determines whether the article belongs to that sector:\n")
        questionnaire[sector_name] = {
            "description": sector_description,
            "questions": []
        }
        print("Proceed to add questions to the sector")
        questionnaire = add_new_questions(questionnaire, sector_name)
        return questionnaire


def add_new_questions(questionnaire: dict, sector_name: str | None = None) -> dict:
    while True:
        if sector_name is None:
            user_prompt = input("Which sector do you want to add questions to?: ")
            if user_prompt in list(questionnaire.keys()):
                sector_name = user_prompt
            else:
                print("Invalid sector")
                continue
        question_prompt = input("Insert the question itself: ")
        question_weight = input("Insert que weight of the question (int): ")
        if not question_weight.isnumeric():
            print("wheigh is NAN. Operation canceled")
            continue
        sector_questions = questionnaire[sector_name]["questions"]
        sector_questions += [{
            "q": question_prompt,
            "w": int(question_weight)
        }]
        if input("Continue adding questions to the sector? (Y/N): ").lower() != "y":
            questionnaire[sector_name]["questions"] = sector_questions
            return questionnaire


def delete_sector(questionnaire: dict) -> dict:
    while True:
        sector_name = input("New Sector Name:\n")
        if sector_name not in list(questionnaire.keys()):
            print("Invalid sector name")
            continue
        questionnaire.pop(sector_name)  # TODO this mutates the argument, be careful
        return questionnaire


def delete_questions(questionnaire: dict, sector_name: str | None = None) -> dict:
    while True:
        if sector_name is None:
            user_prompt = input("Which sector do you want to remove questions from?: ")
            if user_prompt in list(questionnaire.keys()):
                sector_name = user_prompt
            else:
                print("Invalid sector")
                continue
        question_list = questionnaire[sector_name]["questions"]
        for i, q in enumerate(question_list):
            print(f"Question {i}; {q}")
        index_prompt = input("Select index to be deleted (or type 'c' to cancel)")
        if index_prompt.lower() == "c":
            pass
        elif index_prompt.isnumeric() and int(index_prompt) < len(question_list):
            question_list.pop(int(index_prompt))
        if input("Continue deleting questions to the sector? (Y/N): ").lower() != "y":
            questionnaire[sector_name]["questions"] = question_list
            return questionnaire


def get_operation_index(prompt: str, min_index: int, max_index: int) -> int:
    while True:
        user_prompt = input(prompt)
        if not user_prompt.isnumeric():
            print("Input is NAN")
            continue
        index_inputed = int(user_prompt)
        if index_inputed < min_index or index_inputed > max_index:
            print(f"Input is not in required range [{min_index} to {max_index}]")
            continue
        return index_inputed


def print_curr_dictionary(questionnaire: dict) -> dict:
    print(questionnaire)
    return questionnaire

def end_editing(questionnarie: dict) -> None:
    save_questions(questionnarie)
    exit(0)

def save_questions(questionnaire: dict) -> None:
    with open(QUESTIONS_PATH, "w") as fstream:
        json.dump(questionnaire, fp=fstream)


def main_loop():
    questionnaire = open_questions_file()
    action_dict = {
        0: {
            "f": print_curr_dictionary,
            "n": "Print current dictionary"
        },
        1: {
            "f": add_new_sector,
            "n": "Add a new sector"
        },
        2: {
            "f": add_new_questions,
            "n": "Add questions to a sector"
        },
        3: {
            "f": delete_sector,
            "n": "Delete an exisiting sector"
        },
        4: {
            "f": delete_questions,
            "n": "Delete an existing question"
        },
        5: {
            "f": end_editing,
            "n": "End editing and save"
        }
    }
    while True:
        print("Actions:")
        for key in action_dict.keys():
            print(f"{key}: {action_dict[key]["n"]}")
        action_index = get_operation_index(prompt="What operation will be done?: ", min_index=0, max_index=5)
        action = action_dict[action_index]["f"]
        questionnaire = action(questionnaire)


if __name__ == '__main__':
    main_loop()