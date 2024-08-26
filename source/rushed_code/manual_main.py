import json

from data_merger.disaster_merger_1 import ExpedientMerger
from data_merger.disaster_merger_2 import QuartileCuller
from data_merger.disaster_merger_3 import DisasterLinker

from scraping.event import Event
from scraping.questionnaire import Questionnaire

from common.exceptions import IPBanError

from auxiliary_main import culler, disaster_to_dict_factory, generate_search_query

import pandas as pd


def process_events_batches():
    BATCH_SIZE = 3
    INPUT_PATH = "../input-output/merged_expedients_2.csv"
    CONTROL_PATH = "rushed_code/control_data/control.json"
    with open(CONTROL_PATH) as fstream:
        CONTROL_DATA = json.load(fstream)
    df = pd.read_csv(INPUT_PATH, parse_dates=["date"])
    summaries = []
    try:
        while CONTROL_DATA["last_row_processed"] < df.shape[0]:
            print(f"Processing rows "
                  f"{CONTROL_DATA["last_row_processed"]} to "
                  f"{CONTROL_DATA["last_row_processed"] + BATCH_SIZE}"
                  )
            event_list = Event.from_dataframe(
                df.iloc[CONTROL_DATA["last_row_processed"]:(CONTROL_DATA["last_row_processed"] + BATCH_SIZE)]
            )
            print(f"Number of events: {len(event_list)}")
            Event.get_related_news_concurrent(event_list, generate_search_query)
            event_list = Event.filter_out_irrelevant_events(event_list)
            print(f"Useful total events: {len(event_list)}")
            summaries.extend([e.summary_event() for e in event_list])
            CONTROL_DATA["last_row_processed"] += BATCH_SIZE
    except IPBanError:
        print("IP banned stopping procedure")
    except KeyboardInterrupt:
        return
    finally:
        print("Running cleanup")
        if len(summaries) == 0:
            print("No cleanup was needed")
            return
        file_id = CONTROL_DATA['last_output_file_id']
        CONTROL_DATA['last_output_file_id'] += 1
        result_df = pd.DataFrame(data=summaries)
        result_df.to_csv(f"rushed_code/unmerged_data/result_{file_id}.csv")
        with open(CONTROL_PATH, "w+") as fstream:
            json.dump(CONTROL_DATA, fstream)


if __name__ == '__main__':
    process_events_batches()