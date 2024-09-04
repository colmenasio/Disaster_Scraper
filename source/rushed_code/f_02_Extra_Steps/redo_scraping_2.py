import pandas as pd

from common.merge_dictionaries import merge_dicts
from source.scraping.article import Article


# Script to re-use information and links scraped in previous executions of the scraper as a base for a rerun of the
# scraper
# More specifically, this script is supposed to run on a csv file made of event summaries (see Event.summarize_event())
# It re-does the affected_sectors,severity_ratio and answer_ratio collumns
# This version differs in the fact that it does not merge articles in events and instead stores them for future processing

def redo_row(row: dict) -> dict | None:
    """Returns none if the row is considered not useful"""
    # Scrape the data using the Article class in a definely unintended way
    required_columns = ["theme", "start_time", "end_time", "sources", "location", "df_index"]
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
        # If the article is useless, dont bother storing it
        if ((not curr_article.sucessfully_built)
                or len(curr_article.sectors) == 0
                or len(curr_article.severity) == 0
                or len(curr_article.answers) == 0):
            continue
        else:
            articles.append(curr_article)

    if len(articles) == 0:
        return None

    # generate new row
    new_row = row.copy()
    for useless_row in ["affected_sectors", "severity_ratio", "answer_ratio"]:
        if useless_row in new_row:
            new_row.pop(useless_row)
    new_row["sources"] = list({article.id for article in articles})
    return new_row


def redo_df(df: pd.DataFrame) -> pd.DataFrame:
    new_df = pd.DataFrame(columns=["theme", "start_time", "end_time", "sources", "location", "df_index"])
    for i in df.index:
        print(f"redoing id {i}")
        new_row = redo_row(dict(df.iloc[i]))
        if new_row is None:
            continue
        else:
            new_df.loc[i] = new_row
    new_df.reset_index(inplace=True)
    return new_df


def read_csv(filepath) -> pd.DataFrame:
    return pd.read_csv(filepath).drop(columns=["Unnamed: 0"])


if __name__ == '__main__':
    BASE_CSV_FILE_PATH = "rushed_code/f_01_Basic_Scraping/results/result_semi_final_2.csv"
    # BASE_CSV_FILE_PATH = "rushed_code/f_01_Basic_Scraping/unmerged_batches/result_0.csv"
    EVENT_OUTPUT_PATH = "rushed_code/f_02_Extra_Steps/results/events_final_batch_2.csv"
    ARTICLE_INPUT_PATH = "rushed_code/f_02_Extra_Steps/results/articles_final_batch_1.csv"
    ARTICLE_OUTPUT_PATH = "rushed_code/f_02_Extra_Steps/results/articles_final_batch_2.csv"

    # If there is an article cache to load, load it
    if ARTICLE_INPUT_PATH is not None:
        old_cache = pd.read_csv(ARTICLE_INPUT_PATH,
                                converters={key: lambda x: eval(x) for key in ['sectors', 'severity', 'answers']}
                                ).set_index("id")
        Article.load_cache(old_cache)
    og_df = read_csv(BASE_CSV_FILE_PATH)
    new_df = redo_df(og_df)
    new_df.to_csv(path_or_buf=EVENT_OUTPUT_PATH, index=False)
    article_df = Article.dump_cache()
    article_df.to_csv(path_or_buf=ARTICLE_OUTPUT_PATH, index=True)
