import pandas as pd
import numpy as np


# A script to assist on the creation on a province adjacency table
def add_adjacencies(df, names, current_province) -> None:
    """Manually add adjacencies"""
    print(f"Curretly editing {current_province}")
    done = False
    while done is False:
        print(f"Possible names: \n {names}")
        print(f"Currently the provinces adjacent to {current_province} are "
              f"{list(df.loc[df[current_province] == True].index)}")
        prompt = input(f"Input Provinces adjacent to {current_province}:\n").split(", ")
        if all([province in names for province in prompt]):
            df.loc[current_province, prompt] = True
            done = True
        elif prompt == [""]:
            done = True
        else:
            print("Some Provinces were not found")
    print("Success")


def update_df(df, current_province) -> None:
    """Ensures the symmetry of the table"""
    df.loc[:, current_province] = df.loc[current_province, :]
    pass


def generate_map():
    EXPEDIENTS_PATH = "../input-output/merged_expedients.csv"
    print(f"Getting Provinces Names from {EXPEDIENTS_PATH}")
    try:
        names = list(pd.read_csv(EXPEDIENTS_PATH)["province"].drop_duplicates())
        names = sorted(names)
    except Exception as e:
        print("Error ocurred when getting province names")
        exit(1)

    size = len(names)
    adjacency_table = pd.DataFrame(data=np.full((size, size), False),
                                   columns=names,
                                   index=names)
    for province in names:
        # Set each province as adjacent to itself
        adjacency_table.at[province, province] = True
        add_adjacencies(adjacency_table, names, province)
        update_df(adjacency_table, province)

    adjacency_table.to_csv(path_or_buf="../data/provinces_adjacency/adjacency_table.csv")
    print("Adjacency table saved")


if __name__ == '__main__':
    generate_map()
