from source.scraping.questionnaire import Questionnaire
import pandas as pd

OUTPUT_PATH = "rushed_code/results/questions_by_id.csv"

if __name__ == '__main__':
    df = pd.DataFrame.from_dict(Questionnaire.get_question_id_dict(),
                                orient="index",
                                columns=["Question", "Weight", "Sector"])
    df.to_csv(OUTPUT_PATH)