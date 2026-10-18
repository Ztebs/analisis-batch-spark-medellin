"""
Tarea 3 - Big Data (UNAD 202016911)
Procesamiento en batch con Apache Spark (PySpark)

Conjunto de datos: incidentes viales de Medellín (2008-2025).
Pregunta del problema: ¿en qué horas, comunas y tipos de incidente
ocurren los accidentes más graves (con víctimas fatales)?

Uso:
    spark-submit analisis_batch.py accidentes_medellin.csv
"""
import os
import sys
from operator import add

import matplotlib
matplotlib.use("Agg")  # sin pantalla: solo guarda las imágenes
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RUTA = sys.argv[1] if len(sys.argv) > 1 else "accidentes_medellin.csv"
CARPETA_GRAFICOS = "graficos"
CARPETA_TABLAS = "tablas"
os.makedirs(CARPETA_GRAFICOS, exist_ok=True)
os.makedirs(CARPETA_TABLAS, exist_ok=True)

spark = (SparkSession.builder
         .appName("AccidentesMedellinBatch")
         .config("spark.sql.shuffle.partitions", "8")  # pocos datos: menos particiones
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------
# 1. CARGA DE DATOS
# ---------------------------------------------------------------
# El CSV usa ";" como separador y coma como decimal en latitud/longitud.
df = (spark.read
      .option("header", True)
      .option("sep", ";")
      .option("encoding", "UTF-8")
      .csv(RUTA))

nombres = ["llave", "anio", "fecha", "hora", "clase", "gravedad", "direccion",
           "latitud", "longitud", "cod_comuna", "comuna", "cod_barrio", "barrio"]
if len(df.columns) != len(nombres):
    sys.exit(f"Se esperaban {len(nombres)} columnas y hay {len(df.columns)}. "
             "Revisa el separador del CSV.")
df = df.toDF(*nombres)  # nombres simples, sin tildes ni espacios

total_inicial = df.count()
print(f"\n=== 1. CARGA ===\nFilas leídas: {total_inicial:,}")
print("Columnas:", df.columns)
df.printSchema()

# ---------------------------------------------------------------
# 2. LIMPIEZA Y TRANSFORMACIÓN
# ---------------------------------------------------------------
# 2.1 Quitar espacios y convertir "N/D", "Sin inf" y vacíos en nulos
for c in df.columns:
    limpio = F.trim(F.col(c))
    df = df.withColumn(
        c,
        F.when(limpio.isin("", "N/D", "Sin inf"), None).otherwise(limpio))

# 2.2 Convertir tipos de datos
df = (df
      .withColumn("llave", F.col("llave").cast("long"))
      .withColumn("anio", F.col("anio").cast("int"))
      .withColumn("fecha", F.to_date("fecha", "yyyy-MM-dd"))
      .withColumn("hora_dia", F.split("hora", ":").getItem(0).cast("int"))
      .withColumn("latitud", F.regexp_replace("latitud", ",", ".").cast("double"))
      .withColumn("longitud", F.regexp_replace("longitud", ",", ".").cast("double"))
      .withColumn("gravedad", F.upper("gravedad")))

# 2.3 Coordenadas fuera de Medellín se consideran errores
fuera = ~(F.col("latitud").between(6.0, 6.5) & F.col("longitud").between(-75.8, -75.3))
df = (df.withColumn("latitud", F.when(fuera, None).otherwise(F.col("latitud")))
        .withColumn("longitud", F.when(fuera, None).otherwise(F.col("longitud"))))

# 2.4 Quitar duplicados por identificador y filas sin fecha u hora
df = df.dropDuplicates(["llave"])
df = df.filter(F.col("fecha").isNotNull() & F.col("hora_dia").between(0, 23))

# 2.5 Variables nuevas
dias = ["Domingo", "Lunes", "Martes", "Miercoles", "Jueves", "Viernes", "Sabado"]
df = (df
      .withColumn("dia_semana",
                  F.element_at(F.array(*[F.lit(d) for d in dias]), F.dayofweek("fecha")))
      .withColumn("franja",
                  F.when(F.col("hora_dia") < 6, "1-Madrugada (0-5)")
                   .when(F.col("hora_dia") < 12, "2-Mañana (6-11)")
                   .when(F.col("hora_dia") < 18, "3-Tarde (12-17)")
                   .otherwise("4-Noche (18-23)"))
      .withColumn("es_fatal", (F.col("gravedad") == "MUERTO").cast("int")))

df = df.cache()
total = df.count()
print("\n=== 2. LIMPIEZA ===")
print(f"Filas después de limpiar: {total:,} (se descartaron {total_inicial - total:,})")
print("Valores nulos por columna:")
df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]).show(vertical=True)

# ---------------------------------------------------------------
# 3. ANÁLISIS EXPLORATORIO (EDA)
# ---------------------------------------------------------------
print("=== 3. ANÁLISIS EXPLORATORIO ===")

print("Distribución por gravedad:")
por_gravedad = df.groupBy("gravedad").count().orderBy(F.desc("count"))
por_gravedad.show()

print("Incidentes por año (con fatales):")
por_anio = (df.groupBy("anio")
            .agg(F.count("*").alias("incidentes"), F.sum("es_fatal").alias("fatales"))
            .orderBy("anio"))
por_anio.show(30)

print("Por clase de incidente (con % fatales):")
por_clase = (df.groupBy("clase")
             .agg(F.count("*").alias("incidentes"), F.sum("es_fatal").alias("fatales"))
             .withColumn("pct_fatales", F.round(F.col("fatales") * 100 / F.col("incidentes"), 2))
             .orderBy(F.desc("incidentes")))
por_clase.show(truncate=False)

print("Por franja horaria:")
por_franja = (df.groupBy("franja")
              .agg(F.count("*").alias("incidentes"), F.sum("es_fatal").alias("fatales"))
              .withColumn("pct_fatales", F.round(F.col("fatales") * 100 / F.col("incidentes"), 3))
              .orderBy("franja"))
por_franja.show(truncate=False)

print("Por hora del día:")
por_hora = (df.groupBy("hora_dia")
            .agg(F.count("*").alias("incidentes"), F.sum("es_fatal").alias("fatales"))
            .orderBy("hora_dia"))

print("Por día de la semana:")
por_dia = (df.groupBy("dia_semana")
           .agg(F.count("*").alias("incidentes"), F.sum("es_fatal").alias("fatales"))
           .orderBy(F.desc("incidentes")))
por_dia.show()

print("Top 10 comunas con más incidentes:")
top_comunas = (df.filter(F.col("comuna").isNotNull())
               .groupBy("comuna")
               .agg(F.count("*").alias("incidentes"), F.sum("es_fatal").alias("fatales"))
               .orderBy(F.desc("incidentes")).limit(10))
top_comunas.show(truncate=False)

print("Top 10 comunas con más incidentes fatales:")
top_comunas_fatales = (df.filter((F.col("es_fatal") == 1) & F.col("comuna").isNotNull())
                       .groupBy("comuna").count()
                       .orderBy(F.desc("count")).limit(10))
top_comunas_fatales.show(truncate=False)

# ---------------------------------------------------------------
# 4. DEMOSTRACIÓN CON RDD (reduceByKey)
# ---------------------------------------------------------------
print("=== 4. EJEMPLO CON RDD ===")
conteo_rdd = (df.rdd
              .filter(lambda fila: fila["es_fatal"] == 1 and fila["clase"] is not None)
              .map(lambda fila: (fila["clase"], 1))
              .reduceByKey(add))
print("Incidentes fatales por clase (RDD, reduceByKey):")
for clase, n in sorted(conteo_rdd.collect(), key=lambda x: -x[1]):
    print(f"  {clase}: {n}")

# ---------------------------------------------------------------
# 5. VISUALIZACIÓN (se guardan imágenes PNG y tablas CSV)
# ---------------------------------------------------------------
print("\n=== 5. GRÁFICOS ===")
anio_pd = por_anio.toPandas()
clase_pd = por_clase.toPandas()
franja_pd = por_franja.toPandas()
hora_pd = por_hora.toPandas()
dia_pd = por_dia.toPandas()
comunas_pd = top_comunas.toPandas()
comunas_fat_pd = top_comunas_fatales.toPandas()

for nombre, tabla in [("por_anio", anio_pd), ("por_clase", clase_pd),
                      ("por_franja", franja_pd), ("por_hora", hora_pd),
                      ("por_dia", dia_pd), ("top_comunas", comunas_pd),
                      ("top_comunas_fatales", comunas_fat_pd)]:
    tabla.to_csv(f"{CARPETA_TABLAS}/{nombre}.csv", index=False)


def guardar(nombre):
    plt.tight_layout()
    plt.savefig(f"{CARPETA_GRAFICOS}/{nombre}.png", dpi=130)
    plt.close()
    print("  guardado:", f"{CARPETA_GRAFICOS}/{nombre}.png")


plt.figure(figsize=(9, 4.5))
plt.plot(anio_pd["anio"], anio_pd["incidentes"], marker="o", color="#1f77b4")
plt.title("Incidentes viales por año en Medellín")
plt.xlabel("Año"); plt.ylabel("Número de incidentes"); plt.grid(alpha=0.3)
guardar("1_incidentes_por_anio")

plt.figure(figsize=(9, 4.5))
plt.barh(clase_pd["clase"][::-1], clase_pd["incidentes"][::-1], color="#2a9d8f")
plt.title("Incidentes por clase")
plt.xlabel("Número de incidentes")
guardar("2_incidentes_por_clase")

plt.figure(figsize=(9, 4.5))
plt.bar(hora_pd["hora_dia"], hora_pd["incidentes"], color="#e9c46a")
plt.title("Incidentes por hora del día")
plt.xlabel("Hora (0-23)"); plt.ylabel("Número de incidentes")
plt.xticks(range(0, 24))
guardar("3_incidentes_por_hora")

plt.figure(figsize=(9, 4.5))
plt.bar(hora_pd["hora_dia"], hora_pd["fatales"], color="#d62828")
plt.title("Incidentes fatales por hora del día")
plt.xlabel("Hora (0-23)"); plt.ylabel("Incidentes fatales")
plt.xticks(range(0, 24))
guardar("4_fatales_por_hora")

plt.figure(figsize=(9, 4.5))
plt.barh(comunas_pd["comuna"][::-1], comunas_pd["incidentes"][::-1], color="#457b9d")
plt.title("Top 10 comunas con más incidentes")
plt.xlabel("Número de incidentes")
guardar("5_top_comunas")

plt.figure(figsize=(9, 4.5))
plt.barh(comunas_fat_pd["comuna"][::-1], comunas_fat_pd["count"][::-1], color="#d62828")
plt.title("Top 10 comunas con más incidentes fatales")
plt.xlabel("Incidentes fatales")
guardar("6_top_comunas_fatales")

print("\nListo. Resultados en las carpetas 'graficos' y 'tablas'.")
spark.stop()
