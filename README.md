***provisional readme***

# OUTPUTS

The script outputs 2 dataframes:
- The extracted information
- The id to question mapping

### Extracted Information:
Each row contains information about a disaster formed by combining information extracted from various events

Collumns (name and type):
- `Theme (str)`: 
  - The type of disaster 
  - eg: (Tormenta, Desbordamiento, etc...)
- `Locations (list[str])`: 
  - List of affected locations 
  - eg: ([Madrid,Toledo], [Almeria, Granada, Jaen])
- `Start_time (datetime64)` and `End_time (datetime64)`:
  - Self explanatory
- `Events_id (list[int])`:
  - List of the ids of the events that were merged to form this disaster
- `Severity_by_sector (dict[str, float])`:
  - Each key is a sector (Energia, Transporte, etc)
  - Each value is an averaged weighted sum of the severity of each article in each event, according to the answers to the questions in questions.json
  - The value will be in the range [0-1]
  - The sectors that were not affected will not have a key in the dictionary
- `Question_average (dict[int, float])`:
  - Each key corresponds to the id of a question asked
  - Each value is the average response over all articles in all events
  - For example; Lets consider question `1`. If 3 articles answer `True` and 2 answer `False` to question `1`, then the value associated to the key `1` in this dictionary will be `0.6`


### Id to Question Mapping
A simple dataframe associating each id of a question to the question itself and its weight and sector

(The following pseudo-code illustrates it)

`1: question-> "¿Se ha visto afectado el transporte maritimo?", 
    sector->"Transporte"
    weight-> 10
`
