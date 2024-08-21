# Jurisprudencia-Argentina
Scrapper y armado de dataset a partir del [Sistema Argentino de Información Jurídica](http://www.saij.gob.ar)
-

## Uso
```scrapper.py```

Recorre todo SAIJ para:
- Recolectar URLs de todas las entradas hasta la fecha y guardarlas en un txt
- Recolectar de cada URL el contenido asociado y guardarlo en un jsonl
## Argumentos

- `--urls-output`: Archivo de salida para las URLs de los contenidos. Por defecto es `urls.txt`.

- `--dataset-output`: Archivo de salida para el dataset. Por defecto es `dataset.jsonl`.

- `--update`: Solo recolectar el contenido más reciente. Opcional.

- `--data`: Solo recolectar datos de contenido, salteando el paso de recolectar las URLs. Opcional

- `--amount`: Cantidad de resultados por cada búsqueda de URLs. Por defecto 4000.

- `--log-level`: Nivel de registro para la salida de logs. Los valores posibles son `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`. Por defecto es `INFO`.

- `--initial-delay`: Retraso inicial para la limitación de tasa adaptativa. Por defecto es `0.01` segundos.

- `--max-delay`: Retraso máximo para la limitación de tasa adaptativa. Por defecto es `5.0` segundos.

## Formato del dataset
Por ahora, el crawler recopila y guarda los datos de las entradas en el formato que maneja Infojus:
```json
{
  "numero-sumario": "Número de identificación del sumario",
  "materia": "Área del derecho a la que pertenece el caso",
  "timestamp": "Fecha y hora de creación del registro",
  "timestamp-m": "Fecha y hora de la última modificación del registro",
  "sumario": "Resumen del caso",
  "caratula": "Título del caso",
  "descriptores": {
    "descriptor": [
      {
        "elegido": {
          "termino": "Término elegido para describir al caso"
        },
        "preferido": {
          "termino": "Término preferido para describir al caso"
        },
        "sinonimos": {
          "termino": ["Lista de sinónimos"]
        }
      }
    ],
    "suggest": {
      "termino": ["Lista de términos sugeridos"]
    }
  },
  "fecha": "Fecha del caso",
  "instancia": "Instancia judicial",
  "jurisdiccion": {
    "codigo": "Código de la jurisdicción",
    "descripcion": "Descripción de la jurisdicción",
    "capital": "Capital de la jurisdicción",
    "id-pais": "ID del país"
  },
  "numero-interno": "Número interno del caso",
  "provincia": "Provincia donde se lleva el caso",
  "tipo-tribunal": "Tipo de tribunal",
  "referencias-normativas": {
    "referencia-normativa": {
      "cr": "Referencia cruzada",
      "id": "ID de la referencia normativa",
      "ref": "Referencia normativa"
    }
  },
  "fecha-alta": "Fecha de alta del registro",
  "fecha-mod": "Fecha de última modificación del registro",
  "fuente": "Fuente del registro",
  "uid-alta": "UID de alta",
  "uid-mod": "UID de modificación",
  "texto": "Texto completo del caso",
  "id-infojus": "ID de Infojus",
  "titulo": "Título del sumario",
  "guid": "GUID del registro"
}
```