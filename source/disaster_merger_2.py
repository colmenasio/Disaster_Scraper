import pandas as pd
from typing import Callable


# Second step of the merging of disasters. Contains filters, methods to discard unwanted expedients, etc...

def _find_lower_qcuts(df: pd.DataFrame,
                      key: Callable[[pd.DataFrame], pd.Series],
                      total_q: int,
                      lower_n_cuts: int, ) -> pd.Series:
    """Returns a Series of bool values determining wether each row of a dataframe is in the nth qcut or lower according
    to some provided key

    :return: A series. In each entry of the series, True means the correspondent row belongs in the lower n qcuts, False means it is in the upper q-n qcuts"""
    if lower_n_cuts > total_q:
        raise ValueError
    label_names = [f"q{n}" for n in range(total_q)]
    label_lower = [f"q{n}" for n in range(lower_n_cuts)]
    qcuts = pd.qcut(key(df), q=total_q, labels=label_names)
    result = pd.Series.apply(self=qcuts, func=lambda qcut: qcut in label_lower)
    return result


def drop_lower_qcuts(df: pd.DataFrame,
                     key: Callable[[pd.DataFrame], pd.Series],
                     total_q: int,
                     lower_n_cuts: int,
                     inplace: bool = False) -> pd.DataFrame | None:
    """Cuts a dataframe into qcuts and removes the lower N qcuts from the dataframe
    :param df: Target DataFrame
    :param key: Callable returning a Series from the dataframe used as keys in the qcuts creation
    :param total_q: Total number of partitions (qcuts)
    :param lower_n_cuts: Number of lower partitions to be removed
    :param inplace: Do the operation in place or not (Default: False)
    """
    is_lower_qcut = _find_lower_qcuts(df, key, total_q, lower_n_cuts)
    if inplace:
        df.drop(df.index[is_lower_qcut], inplace=True)
        df.reset_index(drop=True, inplace=True)
        return None
    else:
        new_df = df.drop(df.index[is_lower_qcut], inplace=False)
        new_df.reset_index(drop=True, inplace=True)
        return new_df


def index_key(column: str, df: pd.DataFrame) -> pd.Series:
    """Returns a series of keys corresponding to the value of a column in each row"""
    return df[column]


def losses_per_claim_key(df: pd.DataFrame) -> pd.Series:
    """Returns a series of keys corresponding to the ratio of losses per claim row"""
    return df.apply(lambda row: row["total_cost"] / row["claims"], axis=1)

