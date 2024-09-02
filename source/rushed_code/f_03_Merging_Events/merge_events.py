import pandas as pd

from source.rushed_code.f_03_Merging_Events.alt_auxiliary_main import disaster_to_dict_factory
from source.rushed_code.f_03_Merging_Events.summarized_event import SummarizedEvent
from source.data_merger.disaster_merger_3 import DisasterLinker


def read_csv(filepath) -> pd.DataFrame:
    df = pd.read_csv(filepath, converters={
        "affected_sectors": lambda x: eval(x),
        "severity_ratio": lambda x: eval(x),
        "answer_ratio": lambda x: eval(x),
    })
    if "Unnamed: 0" in df.columns:
        df.drop(columns=["Unnamed: 0"], inplace=True)
    df['start_time'] = pd.to_datetime(df['start_time'], errors='raise')
    df['end_time'] = pd.to_datetime(df['end_time'], errors='raise')
    return df


if __name__ == '__main__':
    EVENT_PATH = "rushed_code/f_01_Basic_Scraping/results/result_semi_final.csv"
    df = read_csv(EVENT_PATH)
    summarized_events = [SummarizedEvent(dict(df.iloc[i])) for i in df.index]
    del df
    disaster_to_dict = disaster_to_dict_factory(summarized_events)
    DisasterLinker.INPUT_PATH = "../input-output/merged_expedients_2.csv"
    DisasterLinker.do_startup()
    disaster_list = DisasterLinker.build_initial_disaster_pool()
    DisasterLinker.collapse_disaster_list(disaster_list)
    final_df = DisasterLinker.to_dataframe(disaster_list, disaster_to_dict)
    print(final_df)
