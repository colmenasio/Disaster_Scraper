from unidecode import unidecode

# Tool to convert accented characters like â or á into their ascii counterparts (a, e, ... etc)

def process_text_file(input_file, output_file):
    with open(input_file, 'r', encoding='utf-8') as file:
        text = file.read()

    # Convert accented characters to ASCII
    text = unidecode(text)

    with open(output_file, 'w', encoding='utf-8') as file:
        file.write(text)


# Usage
input_file = 'question_creation.py'
output_file = 'question_creation_converted.py'
process_text_file(input_file, output_file)
