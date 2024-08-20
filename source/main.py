from data_merger.disaster_merger_1 import ExpedientMerger
from data_merger.disaster_merger_2 import QuartileCuller
from data_merger.disaster_merger_3 import DisasterLinker

from scraping.event import Event
from scraping.questionnaire import Questionnaire

from auxiliary_main import culler, disaster_to_dict_factory, generate_search_query

if __name__ == '__main__':
    # First, we merge the raw expedients into articles
    ExpedientMerger.INPUT_PATH = "../input-output/raw_expedients.csv"
    ExpedientMerger.OUTPUT_PATH = "../input-output/all_events.csv"
    ExpedientMerger.merge_csv()

    # Then, we filter the articles by quartile exclusion
    QuartileCuller.INPUT_PATH = "../input-output/all_events.csv"
    QuartileCuller.OUTPUT_PATH = "../input-output/culled_events.csv"

    QuartileCuller.apply_cull_csv_file(culler)

    #exit(0) # AVOID INNECESARY API CALLS WHEN TESTING

    # We extract information about each event in the remaining articles
    Event.INPUT_PATH = "../input-output/culled_events.csv"
    events = Event.from_csv()
    events = Event.extract_info_events(events, generate_search_query)

    # Link the information extrated from articles into disasters
    DisasterLinker.INPUT_PATH = "../input-output/culled_events.csv"
    DisasterLinker.OUTPUT_PATH = "../input-output/results.csv"
    disaster_list = DisasterLinker.build_initial_disaster_pool()
    DisasterLinker.collapse_disaster_list(disaster_list)

    final_df = DisasterLinker.to_dataframe(disaster_list, disaster_to_dict_factory(events))
    questions_id = Questionnaire.get_question_id_dict()

