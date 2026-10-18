# Análisis batch de incidentes viales en Medellín con Apache Spark

Tarea 3 – Procesamiento de Datos con Apache Spark
Curso Big Data (202016911) – Universidad Nacional Abierta y a Distancia (UNAD)

## Problema

¿En qué horas, comunas y tipos de incidente ocurren los accidentes viales más graves (con víctimas fatales) en Medellín?

## Conjunto de datos

- **Nombre:** Fatal Road-Traffic Incident Dataset for Medellín, Colombia (2008–2025)
- **Fuente:** Mendeley Data, https://doi.org/10.17632/r6g5dfnpgh.1
- **Origen de los registros:** Secretaría de Movilidad de Medellín
- **Tamaño:** cerca de 700.000 incidentes (una fila por incidente)
- **Columnas principales:** año, fecha, hora, clase de incidente, gravedad, dirección, latitud, longitud, comuna y barrio

El archivo original viene en Excel (`Fatal_Road_Traffic.xlsx`). Para usarlo con Spark se guardó como CSV UTF-8 con el nombre `accidentes_medellin.csv` y se cambió el formato de la fecha a `yyyy-mm-dd`. El CSV resultante usa `;` como separador y coma como decimal en latitud y longitud.

> El archivo de datos no se sube a este repositorio por su tamaño. Descárgalo desde el enlace de Mendeley y conviértelo como se explica arriba.

## Qué hace el código (`analisis_batch.py`)

1. **Carga** el CSV con Spark y renombra las columnas.
2. **Limpia y transforma:** convierte `N/D` y `Sin inf` en nulos, corrige las coordenadas con coma decimal, descarta coordenadas fuera de Medellín, elimina duplicados y crea variables nuevas (día de la semana, franja horaria e indicador de incidente fatal).
3. **Análisis exploratorio con DataFrames:** incidentes por año, clase, franja horaria, hora, día de la semana y comuna, con porcentaje de incidentes fatales.
4. **Ejemplo con RDD:** conteo de incidentes fatales por clase con `map` y `reduceByKey`.
5. **Visualización:** guarda 6 gráficos en `graficos/` y las tablas en `tablas/`.

## Requisitos

- Python 3
- Apache Spark 3.5.x (PySpark)
- matplotlib (`pip install matplotlib`)

## Cómo ejecutarlo

```bash
spark-submit analisis_batch.py accidentes_medellin.csv
```

Al terminar se crean las carpetas `graficos/` (imágenes PNG) y `tablas/` (resultados en CSV).

## Autor

Esteban Rodríguez Cardozo – Ingeniería de Sistemas, UNAD.
