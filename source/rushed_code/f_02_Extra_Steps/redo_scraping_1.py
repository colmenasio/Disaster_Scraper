import pandas as pd

from common.merge_dictionaries import merge_dicts
from source.scraping.article import Article


# Script to re-use information and links scraped in previous executions of the scraper as a base for a rerun of the
# scraper
# More specifically, this script is supposed to run on a csv file made of event summaries (see Event.summarize_event())
# It re-does the affected_sectors,severity_ratio and answer_ratio collumns

def redo_row(row: dict) -> dict | None:
    """Returns none if the row is considered not useful"""
    # Scrape the data using the Article class in a definely unintended way
    required_columns = ["theme", "start_time", "end_time", "sources", "location", "df_index",
                        "affected_sectors", "severity_ratio", "answer_ratio"]
    if any([required_column not in row for required_column in required_columns]):
        raise ValueError(f"The base file is missing necesary collumns (Need the following collumns {required_columns},"
                         f" note that affected_sectors,severity_ratio and answer_ratio may be 'None')")
    articles = []
    for link in eval(row["sources"]):
        curr_article = Article(
            title_arg=link,
            source_url_arg=None,
            source_name_arg=None,
            date_arg=None,
            link_arg=link
        )
        curr_article.process_article()
        if curr_article.sucessfully_built:
            articles.append(curr_article)

    # Merge articlesç
    def max_mean_min(l: list) -> tuple:
        return max(l), sum(l) / len(l), min(l)

    row["affected_sectors"] = list({s for a in articles for s in a.sectors})
    severities_generator = (a.severity for a in articles)
    row["severity_ratio"] = merge_dicts(severities_generator, merging_operator=max_mean_min)
    answers_generator = (a.answers for a in articles)
    row["answer_ratio"] = merge_dicts(answers_generator, merging_operator=max_mean_min)

    # Check if the row is useful
    if len(row["affected_sectors"]) == 0 or len(row["severity_ratio"]) == 0 or len(row["answer_ratio"]) == 0:
        return None
    return row


def redo_df(df: pd.DataFrame) -> None:
    useless_rows = []
    for i in df.index:
        print(f"redoing id {i}")
        new_row = redo_row(dict(df.iloc[i]))
        if new_row is None:
            useless_rows.append(i)
        else:
            df.iloc[i] = new_row
    df.drop(index=useless_rows, inplace=True)

def read_csv(filepath) -> pd.DataFrame:
    return pd.read_csv(filepath).drop(columns=["Unnamed: 0"])


if __name__ == '__main__':
    # BASE_CSV_FILE_PATH = "rushed_code/f_01_Basic_Scraping/results/events_semi_final.csv"
    BASE_CSV_FILE_PATH = "rushed_code/f_01_Basic_Scraping/unmerged_data/result_0.csv"
    OUTPUT_PATH = "rushed_code/f_02_Extra_Steps/results/events_final.csv"
    dataframe = read_csv(BASE_CSV_FILE_PATH)
    redo_df(dataframe)
    dataframe.to_csv(path_or_buf=OUTPUT_PATH, index=False)
