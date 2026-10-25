# -*- coding: utf-8 -*-
import os
import csv
import random
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum as _sum, round as _round, desc, when

# 1. Generar dataset de prueba si no existe
def asegurar_dataset_csv(ruta_archivo):
    if not os.path.exists(ruta_archivo):
        print(f"[INFO] Generando conjunto de datos en: {ruta_archivo}...")
        origenes = ["Bogota", "Medellin", "Cali", "Barranquilla", "Mosquera", "Bucaramanga"]
        destinos = ["Cartagena", "Bogota", "Cali", "Medellin", "Santa Marta", "Pereira"]
        tipos_carga = ["Alimentos", "Electrodomesticos", "Textiles", "Materiales Construccion", "Tecnologia"]
        
        with open(ruta_archivo, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["id_despacho", "origen", "destino", "tipo_carga", "peso_toneladas", "costo_flete_cop", "tiempo_horas", "estado_entrega"])
            for i in range(1, 5001):
                peso = round(random.uniform(1.5, 35.0), 2) if random.random() > 0.03 else None
                costo = round(random.uniform(800000, 6500000), 0) if random.random() > 0.02 else -100
                tiempo = round(random.uniform(2.0, 48.0), 1)
                origen = random.choice(origenes)
                destino = random.choice([d for d in destinos if d != origen])
                carga = random.choice(tipos_carga)
                estado = random.choice(["Entregado", "Entregado", "En Transito", "Retrasado"])
                writer.writerow([i, origen, destino, carga, peso, costo, tiempo, estado])
        print("[INFO] Dataset creado con éxito.")

# 2. Inicializar Spark Session
spark = SparkSession.builder \
    .appName("Tarea3_Batch_Logistica") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
ruta_csv = "datos_logistica_colombia.csv"
asegurar_dataset_csv(ruta_csv)

# 3. Carga y Limpieza
df_raw = spark.read.option("header", "true").option("inferSchema", "true").csv(ruta_csv)
df_clean = df_raw.dropna(subset=["peso_toneladas", "costo_flete_cop"]).filter((col("peso_toneladas") > 0) & (col("costo_flete_cop") > 0))

df_transformed = df_clean.withColumn(
    "costo_por_tonelada", _round(col("costo_flete_cop") / col("peso_toneladas"), 2)
)

print("\n--- RESULTADOS DEL ANÁLISIS BATCH ---")
df_transformed.groupBy("tipo_carga").agg(
    count("id_despacho").alias("total_viajes"),
    _round(avg("costo_flete_cop"), 2).alias("flete_promedio")
).show()

spark.stop()
