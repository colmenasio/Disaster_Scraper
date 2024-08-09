from data_merger.disaster_merger_1 import ExpedientMerger
from data_merger.disaster_merger_2 import QuartileCuller
from data_merger.disaster_merger_3 import DisasterLinker

from scraping.event import Event
from scraping.questionnaire import Questionnaire

from auxiliary_main import culler, disaster_to_dict

if __name__ == '__main__':
    # First, we merge the raw expedients into events
    ExpedientMerger.INPUT_PATH = "../input-output/raw_expedients.csv"
    ExpedientMerger.OUTPUT_PATH = "../input-output/all_events.csv"
    ExpedientMerger.merge_csv()

    # Then, we filter the events by quartile exclusion
    QuartileCuller.INPUT_PATH = "../input-output/all_events.csv"
    QuartileCuller.OUTPUT_PATH = "../input-output/culled_events.csv"

    QuartileCuller.apply_cull_csv_file(culler)

    # We extract information about each event in the remaining events
    Event.INPUT_PATH = "../input-output/culled_events.csv"
    Event.OUTPUT_PATH = "../input-output/event_information.csv"
    events = Event.from_csv()
    Event.extract_info_to_csv(events)

    # Link the information extrated from events into disasters
    DisasterLinker.INPUT_PATH = "../input-output/culled_events.csv"
    DisasterLinker.OUTPUT_PATH = "../input-output/results.csv"
    disaster_list = DisasterLinker.build_initial_disaster_pool()
    DisasterLinker.collapse_disaster_list(disaster_list)

    final_df = DisasterLinker.to_dataframe(disaster_list, disaster_to_dict)
    questions_id = Questionnaire.get_question_id_dict()

