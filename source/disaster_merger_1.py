import pandas as pd
import numpy as np
import datetime


# A sligth modification of the script provided by matheus. Merges by disaster type, date and location

def merge_css_1():
    # Importing the disaster data
    disaster_df = pd.read_csv(filepath_or_buffer="../input-output/raw_expedients.csv",
                           sep=";",
                           converters={" COSTE TOTAL ": lambda x: x.replace(".", "")},
                           encoding='utf-8')

    # Dropping unnecessary columns and adjusting the data
    disaster_df.drop(
        columns=['ID RMC', 'ID CAUSA SINIESTRO', 'MUNICIPIO', 'POBLACION',
                 'CODIGO POSTAL', 'CLASE RIESGO N1', 'CLASE RIESGO N2'],
        inplace=True
    )

    # Renaming Remaining Collumns for conviniency
    disaster_df.rename(columns={"FECHA SINIESTRO": "date",
                                "CAUSA SINIESTRO": "disaster",
                                "PROVINCIA": "province",
                                " COSTE TOTAL ": "total_cost"},
                       inplace=True)

    # Replacing specific province names for consistency
    disaster_df['date'] = pd.to_datetime(disaster_df['date'], dayfirst=True)
    disaster_df = disaster_df.replace({'province': {'Alicante/Alacant': 'Alicante',
                                              'Araba/Álava': 'Araba',
                                              'Balears, Illes': 'Illes Balears',
                                              'Coruña, A': 'A Coruña',
                                              'Rioja, La': 'La Rioja',
                                              'Palmas, Las': 'Las Palmas',
                                              'Castellón/Castelló': 'Castellón',
                                              'Valencia/València': 'Valencia'}})
    disaster_df['province'] = disaster_df['province'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode(
        'utf-8')
    disaster_df['claims'] = disaster_df['province']
    disaster_df["total_cost"] = pd.to_numeric(disaster_df["total_cost"], errors="coerce")

    aggregation_dict = {"total_cost": "sum", "claims": "count"}

    # Adding sum aggregation for all other columns except date, disaster, province, total_cost, and claims
    for col in disaster_df.columns:
        if col not in ['date', 'disaster', 'province', 'total_cost', 'claims']:
            aggregation_dict[col] = 'sum'

    # Grouping the dataframe by date, disaster, and province, and applying the aggregation dictionary
    disaster_df = disaster_df.groupby(['date', 'disaster', 'province']).agg(aggregation_dict).reset_index()
    disaster_df = disaster_df[disaster_df["total_cost"].notnull()]
    disaster_df = disaster_df.round(2).reset_index(drop=True)
    df = disaster_df

    # Sort the dataframe by province, disaster, and date to prepare for duration calculation
    df = df.sort_values(by=['province', 'disaster', 'date']).reset_index(drop=True)

    # Convert the 'date' column to datetime format
    df['date'] = pd.to_datetime(df['date'], errors='raise')

    # Calculate the difference in days between consecutive rows
    df['dif'] = df['date'].diff().dt.days

    # Replace 0s in 'dif' with 1 to ensure at least a one-day duration
    df['dif'] = df['dif'].replace(0, 1)

    # Add a 'notes' column initialized with empty strings
    df['notes'] = ''

    # Define a function to mark the start ('Min') and end ('Max') of each event duration
    def my_func(x):
        a = df[x:][df.loc[x:, 'dif'] != 1]
        if len(a) > 0:
            df.loc[[x - 1, a.index[0] - 1], 'notes'] = ['Min', 'Max']
            df.loc[x - 1: a.index[0] - 1, 'dif'] = 1
        else:
            df.loc[[x - 1, len(df) - 1], 'notes'] = ['Min', 'Max']
            df.loc[x - 1: len(df) - 1, 'dif'] = 1

    # Apply the function to mark 'Min' and 'Max' notes for each event duration
    [my_func(i) for i in range(1, len(df)) if df.loc[i - 1, 'dif'] != df.loc[i, 'dif'] and df.loc[i, 'dif'] == 1]

    # Mark rows with a single date as 'Singledate' in the 'notes' column
    df.loc[df[df['dif'] != 1].index, 'notes'] = 'Singledate'

    df['duration'] = ''
    df['losses_day'] = ''
    count = total_cost = claims = 0

    # Iterate over each row to calculate duration, total cost, and daily losses
    for i in range(0, len(df)):
        if df.loc[i, 'notes'] == 'Min' or df.loc[i, 'notes'] == '':
            count = count + 1
            total_cost = total_cost + df.loc[i, 'total_cost']
            claims = claims + df.loc[i, 'claims']

        elif df.loc[i, 'notes'] == 'Max':
            count = count + 1
            total_cost = total_cost + df.loc[i, 'total_cost']
            claims = claims + df.loc[i, 'claims']

            df.loc[i, 'duration'] = count
            df.loc[i, 'total_cost'] = total_cost
            df.loc[i, 'claims'] = claims
            df.loc[i, 'losses_day'] = total_cost / count
            count = total_cost = claims = 0
        else:
            count = 0
            df.loc[i, 'duration'] = 1
            df.loc[i, 'losses_day'] = df.loc[i, 'total_cost']

    df = df.loc[df['duration'] != '']
    df = df.drop(columns=['dif', 'notes'])

    df.to_csv("../input-output/merged_expedients_1.csv")
    print("INFO: Merge by disaster type and date completed succesfully")


if __name__ == '__main__':
    merge_css_1()
