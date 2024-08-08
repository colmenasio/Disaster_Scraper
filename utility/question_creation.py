import json

# Crear el diccionario con los sectores y preguntas
preguntas_por_sector = {
    "Energia": [
        "?Ha habido cortes?",
        "?Numero de personas sin servicio?",
        "Gravedad:(Cortes intermitentes = leve, cortes de varias horas = grave, cortes de varios dias = muy grave)"
    ],
    "Transporte": [
        "?Que tipo de transporte identificas?(Aereo, Ferrocarril, Carretera, Metro, Maritimo",
        {
            "Tipo de transporte": [
                {"nombre": "Transporte aereo",
                 "Preguntas": "?Numero de vuelos cancelados?\
             ?Numero de vuelos atrasados?\
             ?Numero de vuelos desviados?"
                 },
                {"nombre": "Transporte Ferrocarril",
                 "Preguntas": "?Ha habido cortes de vias?\
              Gravedad indica cuantos cortes ha habido de cada nivel:(Cortes breves = leve, cortes de varias horas = grave, cortes de varios dias = muy grave)\
              ?Cuantas lineas han sido afectadas?\
              ?Cuantos pasajeros han sido afectados?"
                 },
                {"nombre": "Transporte Carretera",
                 "Preguntas": "?Ha habido cortes de vias?\
              Gravedad, indica cuantos cortes ha habido de cada nivel:(Cortes breves = leve, cortes de varias horas = grave, cortes de varios dias = muy grave)\
              ?Cuantas lineas han sido afectadas?\
              ?Cuantos pasajeros han sido afectados?"
                 },
                {"nombre": "Transporte Metro",
                 "Preguntas": "?Ha habido cortes de vias?\
              Gravedad indica cuantos cortes ha habido de cada nivel:(Cortes breves = leve, cortes de varias horas = grave, cortes de varios dias = muy grave)\
              ?Cuantas lineas han sido afectadas?\
              ?Cuantos pasajeros han sido afectados?"
                 },
                {"nombre": "Transporte Maritimo",
                 "Preguntas": "?Han sido afectados los puertos? Si es asi indicalo."
                 }
            ]
        }
    ],
    "Sanidad": [
        "?Hay hospitales o centros de salud afectados?\
         ?Numero de pacientes afectados?\
         ?Falta de personal o recursos medicos?\
         ?se ha quedado sin luz algun hospital?\
         ?Se ha inundado algun hospital?"
    ],
    "Agua potable": [
        "?Se ha interrumpido el suministro de agua?\
         ?Cuantas personas estan afectadas?\
         ?Que areas estan afectadas?\
         ?Hay problemas de contaminacion del agua?\
         Gravedad: (Interrupciones breves=leve, interrupciones de varias horas=grave, interrupciones de varios dias=muy grave)"
    ],
    "Aguas residuales": [
        "?Se ha interrumpido la recogida o tratamiento de aguas residuales?\
         ?Hay areas afectadas por la falta de tratamiento de aguas residuales?\
         ?Se ha reportado contaminacion en cuerpos de agua?\
         Gravedad: (Interrupciones breves=leve, interrupciones de varias horas=grave, interrupciones de varios dias=muy grave)"
    ],
    "Comunicacion": [
        "?Como de severa ha sido la interrupcion en las comunicaciones?"
    ],
    "Produccion, transformacion y distribucion de alimentos": [
        "?Hay interrupciones en la cadena de suministro de alimentos?\
         ?Que empresas alimentarias estan afectadas?\
         ?Hay escasez de alimentos en areas afectadas?\
         ?Se han visto afectadas las plantas de produccion o transformacion?\
         Gravedad: (Interrupciones breves=leve, interrupciones de varias horas=grave, interrupciones de varios dias=muy grave)"
    ]
}

# Guardar el diccionario como JSON en un archivo
with open("../data/gpt_parser_data/preguntas_por_sector.json", "w") as archivo_json:
    json.dump(preguntas_por_sector, archivo_json, indent=4)

# TODO redo the questions so that the format is a dictionary of lists