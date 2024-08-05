import json

# Crear el diccionario con los sectores y preguntas
preguntas_por_sector = {
    "Energía": [
        "¿Ha habido cortes?\
         ¿Número de personas sin servicio?\
         Gravedad:(Cortes intermitentes = leve, cortes de varias horas = grave, cortes de varios días = muy grave)"
    ],
    "Transporte": [
        "¿Qué tipo de transporte identificas?(Aéreo, Ferrocarril, Carretera, Metro, Marítimo",
        {
            "Tipo de transporte": [
                {"nombre": "Transporte aéreo",
                 "Preguntas": "¿Número de vuelos cancelados?\
             ¿Número de vuelos atrasados?\
             ¿Número de vuelos desviados?"
                 },
                {"nombre": "Transporte Ferrocarril",
                 "Preguntas": "¿Ha habido cortes de vías?\
              Gravedad indica cuantos cortes ha habido de cada nivel:(Cortes breves = leve, cortes de varias horas = grave, cortes de varios días = muy grave)\
              ¿Cuantas líneas han sido afectadas?\
              ¿Cuantos pasajeros han sido afectados?"
                 },
                {"nombre": "Transporte Carretera",
                 "Preguntas": "¿Ha habido cortes de vías?\
              Gravedad, indica cuantos cortes ha habido de cada nivel:(Cortes breves = leve, cortes de varias horas = grave, cortes de varios días = muy grave)\
              ¿Cuantas líneas han sido afectadas?\
              ¿Cuantos pasajeros han sido afectados?"
                 },
                {"nombre": "Transporte Metro",
                 "Preguntas": "¿Ha habido cortes de vías?\
              Gravedad indica cuantos cortes ha habido de cada nivel:(Cortes breves = leve, cortes de varias horas = grave, cortes de varios días = muy grave)\
              ¿Cuantas líneas han sido afectadas?\
              ¿Cuantos pasajeros han sido afectados?"
                 },
                {"nombre": "Transporte Marítimo",
                 "Preguntas": "¿Han sido afectados los puertos? Si es así indicalo."
                 }
            ]
        }
    ],
    "Sanidad": [
        "¿Hay hospitales o centros de salud afectados?\
         ¿Número de pacientes afectados?\
         ¿Falta de personal o recursos médicos?\
         ¿se ha quedado sin luz algún hospital?\
         ¿Se ha inundado algún hospital?"
    ],
    "Agua potable": [
        "¿Se ha interrumpido el suministro de agua?\
         ¿Cuántas personas están afectadas?\
         ¿Qué áreas están afectadas?\
         ¿Hay problemas de contaminación del agua?\
         Gravedad: (Interrupciones breves=leve, interrupciones de varias horas=grave, interrupciones de varios días=muy grave)"
    ],
    "Aguas residuales": [
        "¿Se ha interrumpido la recogida o tratamiento de aguas residuales?\
         ¿Hay áreas afectadas por la falta de tratamiento de aguas residuales?\
         ¿Se ha reportado contaminación en cuerpos de agua?\
         Gravedad: (Interrupciones breves=leve, interrupciones de varias horas=grave, interrupciones de varios días=muy grave)"
    ],
    "Comunicación": [
        "¿Como de severa ha sido la interrupcion en las comunicaciones?"
    ],
    "Producción, transformación y distribución de alimentos": [
        "¿Hay interrupciones en la cadena de suministro de alimentos?\
         ¿Qué empresas alimentarias están afectadas?\
         ¿Hay escasez de alimentos en áreas afectadas?\
         ¿Se han visto afectadas las plantas de producción o transformación?\
         Gravedad: (Interrupciones breves=leve, interrupciones de varias horas=grave, interrupciones de varios días=muy grave)"
    ]
}

# Guardar el diccionario como JSON en un archivo
with open("../data/gpt_parser_data/preguntas_por_sector.json", "w") as archivo_json:
    json.dump(preguntas_por_sector, archivo_json, indent=4)
