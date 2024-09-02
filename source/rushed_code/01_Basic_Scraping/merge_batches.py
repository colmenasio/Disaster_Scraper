import os
import pandas as pd
from typing import Literal

BATCHES_DIR = "rushed_code/01_Basic_Scraping/unmerged_data"
RESULT_DIR = "rushed_code/01_Basic_Scraping/results"
RESULT_NAME = "result_semi_final"
FileExtension = Literal["csv", "xlsx"]


def read_csv(filepath) -> pd.DataFrame:
    return pd.read_csv(filepath).drop(columns=["Unnamed: 0"])


def do_merge(result_format: FileExtension) -> None:
    filenames = filter(lambda x: x != ".gitignore", os.listdir(BATCHES_DIR))
    df_list = (read_csv(f"{BATCHES_DIR}/{filename}") for filename in filenames)
    df = pd.concat(df_list, ignore_index=True)
    df.sort_values(by="df_index")
    result_path = f"{RESULT_DIR}/{RESULT_NAME}.{result_format}"
    if result_format == "csv":
        df.to_csv(result_path)
    elif result_format == "xlsx":
        raise NotImplementedError("xlsx conversion not implemented yet")
        with pd.ExcelWriter(result_path, mode='w') as writer:
            df.to_excel(writer, sheet_name='Results')
    else:
        raise ValueError("result_format not recognized")


if __name__ == '__main__':
    do_merge("csv")
