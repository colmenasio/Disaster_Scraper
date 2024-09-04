import pandas as pd


# Removing useless articles and disasters and re-indexing

def do_article_cleanup(a: pd.DataFrame) -> pd.DataFrame:
    # Remove unecessary articles
    return a[(a['answers'].apply(len) > 0)]


def do_article_relabeling(a: pd.DataFrame, e: pd.DataFrame) -> None:
    # Relabel articles
    old_ids = a.index
    a.index = range(a.shape[0])
    a.index.name = "id"
    new_ids = a.index
    relabeling = {o: n for o, n in zip(old_ids, new_ids)}

    # Update indexes in events
    for i in e.index:
        old_source_ids = e.loc[i]["sources"]
        new_sources_ids = [relabeling[old_id] for old_id in old_source_ids if old_id in relabeling]
        e.at[i, "sources"] = new_sources_ids


def do_event_cleanup(e: pd.DataFrame) -> pd.DataFrame:
    return e[(e['sources'].apply(len) > 0)]


if __name__ == '__main__':
    # EVENT_INPUT_PATH = "rushed_code/f_02_Extra_Steps/results/events_final_raw.csv"
    # ARTICLE_INPUT_PATH = "rushed_code/f_02_Extra_Steps/results/articles_final_raw.csv"
    # EVENT_OUTPUT_PATH = "rushed_code/f_03_Merging_Events/results/events_final_clean.csv"
    # ARTICLE_OUTPUT_PATH = "rushed_code/f_03_Merging_Events/results/articles_final_clean.csv"

    EVENT_INPUT_PATH = "rushed_code/f_03_Merging_Events/results/events_final_clean.csv"
    ARTICLE_INPUT_PATH = "rushed_code/f_03_Merging_Events/results/articles_final_clean.csv"
    EVENT_OUTPUT_PATH = "rushed_code/f_03_Merging_Events/results/events_final_clean.csv"
    ARTICLE_OUTPUT_PATH = "rushed_code/f_03_Merging_Events/results/articles_final_clean.csv"

    articles = pd.read_csv(ARTICLE_INPUT_PATH,
                           converters={key: lambda x: eval(x) for key in ['sectors', 'severity', 'answers']}
                           ).set_index("id")
    events = pd.read_csv(EVENT_INPUT_PATH,
                         converters={"sources": lambda x: eval(x)})
    clean_articles = do_article_cleanup(articles)
    do_article_relabeling(clean_articles, events)
    clean_events = do_event_cleanup(events)

    clean_articles.to_csv(ARTICLE_OUTPUT_PATH)
    clean_events.to_csv(EVENT_OUTPUT_PATH, index=False)