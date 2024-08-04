import pandas as pd
from source.data_merger.disaster_merger_3 import DisasterLinker


def culler(df: pd.DataFrame) -> pd.DataFrame:
    # For now, the culler does nothing as we don't know what parameter to base the culling on
    return df


def disaster_to_dict(disaster: DisasterLinker) -> dict:
    # Define how to generate a row for the result dataframe from a disaster instace (a cluster of events)
    pass
