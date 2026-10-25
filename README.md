# Tarea 3 - Big Data: Procesamiento Batch y Streaming (UNAD)

## Descripción de la Solución
Este repositorio contiene el desarrollo práctico correspondiente a la Tarea 3 del curso de Big Data de la Universidad Nacional Abierta y a Distancia (UNAD). El proyecto aborda dos componentes principales de procesamiento de datos:
1. Procesamiento Batch: Implementado mediante un script en PySpark para el análisis de grandes volúmenes de datos por lotes (batch processing).
2. Streaming en Tiempo Real: Configuración e integración de un clúster de Apache Kafka (como productor de eventos de sensores) con Spark Streaming (como consumidor y procesador analítico en ventanas de tiempo).

---

## Requisitos del Entorno
* Máquina Virtual Ubuntu en VirtualBox (vboxuser@bigdata).
* Apache Spark (v3.3.2 / v3.6.2).
* Apache Kafka (v3.6.2 o superior).
* Python 3.x con las librerías pyspark y kafka-python.

---

## Instrucciones de Ejecución

### 1. Clonar el Repositorio
Abre tu terminal y clona este repositorio en tu entorno local ejecutando:
git clone https://github.com/JDRangell/tarea3-bigdata-unad.git
cd tarea3-bigdata-unad

### 2. Ejecución del Procesamiento Batch
Para ejecutar el script de procesamiento por lotes con PySpark, utiliza el siguiente comando en la terminal:
python3 procesamiento_batch.py

### 3. Ejecución del Flujo en Tiempo Real (Kafka + Spark Streaming)
Para poner en marcha el pipeline de streaming, asegúrate de tener encendidos ZooKeeper y Kafka, y luego ejecuta los siguientes scripts en terminales separadas:

- Iniciar el Productor de Kafka:
python3 kafka_producer.py

- Iniciar el Consumidor de Spark Streaming:
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 spark_streaming_consumer.py

---

## Monitoreo y Evidencias
- Spark UI: Puedes visualizar el rendimiento de los Jobs, Stages y tareas en tiempo real accediendo desde tu navegador a la URL: http://localhost:4040
