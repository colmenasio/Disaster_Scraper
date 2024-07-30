from typing import Callable
import pandas as pd


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
    try:
        qcuts = pd.qcut(key(df), q=total_q, labels=label_names)
    except ValueError as e:
        print(f"The data in the dataframe is not spread enough to make {total_q} qcuts")
        raise e
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


if __name__ == '__main__':
    og_df = pd.read_csv("../input-output/merged_expedients_1.csv")
    og_df.drop(columns=["Unnamed: 0"], inplace=True)
    number_of_qcuts = 10
    drop_lower_n = 2
    df_filtering_by_total_cost = drop_lower_qcuts(df=og_df,
                                                  key=lambda df: index_key("total_cost", df),
                                                  total_q=number_of_qcuts,
                                                  lower_n_cuts=drop_lower_n,
                                                  )
    df_filtering_by_losses_day = drop_lower_qcuts(df=og_df,
                                                  key=lambda df: index_key("losses_day", df),
                                                  total_q=number_of_qcuts,
                                                  lower_n_cuts=drop_lower_n,
                                                  )
    df_filtering_by_claims = drop_lower_qcuts(df=og_df,
                                              key=lambda df: index_key("claims", df),
                                              total_q=2,
                                              lower_n_cuts=1,
                                              )
    df_filtering_by_loss_claim_ratio = drop_lower_qcuts(df=og_df,
                                                        key=losses_per_claim_key,
                                                        total_q=number_of_qcuts,
                                                        lower_n_cuts=drop_lower_n,
                                                        )
    print(og_df.sort_values(by="total_cost"))
    print(og_df.shape)
    print(df_filtering_by_total_cost.sort_values(by="total_cost"))
    print(df_filtering_by_total_cost.shape)