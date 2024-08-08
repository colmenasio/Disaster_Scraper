# GUIDELINES FOR FORMATTING QUESTIONS/SECTORS

For scraping purposes, the script (`article.py`) will try to answer a series of questions 
regarding several infrastructure sectors.

The sectors and their respective associated questions are specified in `questions.json`

The format is the following:

## Format

```json
{
  "Energia": {
    "description": "¿Ha afectado el desastre a la produccion o al suministro de energia eléctrica?",
    "questions": [
      "Question1",
      "Question2"
    ]
  },
  "Transporte": { ... 
  }
}
```

Each entry in `questions.json` correspond to a sector.

It`s value must be a dictionary containing 2 keys:
- `"description"`: A yes/no question that broadly encapsulates the meaning of the sector
- `"questions"`: A list of specific questions that will be asked about articles that are related that sector in question 