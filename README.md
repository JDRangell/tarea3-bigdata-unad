# Tarea 3 - Big Data: Procesamiento Batch y Streaming (UNAD)

## 1. Descripción del proyecto

Este repositorio contiene la implementación práctica de la Tarea 3 del curso Big Data de la Universidad Nacional Abierta y a Distancia (UNAD). Se desarrollan dos procesos: procesamiento por lotes con Apache Spark y procesamiento de eventos en tiempo real mediante Apache Kafka y Spark Streaming.

El flujo en tiempo real utiliza datos simulados de sensores con temperatura, humedad, identificador y marca de tiempo.

## 2. Archivos del proyecto

- `procesamiento_batch.py`: realiza el procesamiento por lotes de los datos logísticos.
- `kafka_producer.py`: genera eventos simulados de sensores y los publica en Kafka.
- `spark_streaming_consumer.py`: consume los eventos de Kafka y calcula promedios de temperatura y humedad por sensor mediante ventanas de tiempo.

## 3. Requisitos

- Ubuntu ejecutado en una máquina virtual.
- Python 3.
- Apache Spark.
- Apache Kafka.
- Biblioteca `kafka-python`.
- Conector de Spark para Kafka compatible con la versión de Spark instalada.

## 4. Instalación de dependencias

Instalar la biblioteca del productor:

```bash
pip install kafka-python
```

Verificar que los tres archivos Python estén en la carpeta del proyecto antes de ejecutar los comandos.

## 5. Ejecución del procesamiento por lotes

Desde la carpeta del proyecto, ejecutar:

```bash
python3 procesamiento_batch.py
```

El programa procesa los datos logísticos, realiza limpieza y transformaciones y presenta resultados agrupados por tipo de carga.

Si el programa genera datos sintéticos cuando no encuentra un archivo de entrada, los resultados deben identificarse como simulados y no como registros oficiales del RNDC.

## 6. Ejecución de Kafka y Spark Streaming

Primero, iniciar los servicios de Kafka y ZooKeeper si la instalación utilizada requiere ZooKeeper.

Comprobar que exista el tópico:

```bash
sudo /opt/Kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

El tópico utilizado en esta práctica es `sensor_data`.

### Terminal 1: productor

Desde la carpeta del proyecto, ejecutar:

```bash
python3 kafka_producer.py
```

El productor enviará eventos simulados con datos de sensores.

### Terminal 2: consumidor de Spark Streaming

Ejecutar:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_streaming_consumer.py
```

Este comando corresponde a la versión del conector utilizada en la prueba práctica. Si la versión instalada de Spark es diferente, debe verificarse la compatibilidad del conector antes de cambiarla.

El consumidor lee los eventos, procesa los datos y muestra promedios de temperatura y humedad por sensor en ventanas de tiempo.

## 7. Monitoreo y evidencias

Mientras Spark Streaming esté ejecutándose, se puede consultar la interfaz web de Spark desde un navegador del equipo anfitrión:

`http://192.168.1.7:4040`

La dirección IP puede cambiar según la configuración de red de la máquina virtual. Si no se puede acceder, se debe comprobar la IP actual de Ubuntu y que la aplicación Spark continúe activa.

Se recomienda registrar evidencias de:

- Ejecución y resultados del procesamiento por lotes.
- Eventos generados por el productor Kafka.
- Resultados calculados por Spark Streaming.
- Interfaz de monitoreo de Spark, si está disponible.

## 8. Arquitectura general

1. El productor genera eventos de sensores.
2. Kafka recibe y almacena temporalmente los eventos en el tópico `sensor_data`.
3. Spark Streaming consume los eventos.
4. Spark transforma y agrupa los registros por sensor y ventana de tiempo.
5. Los resultados se muestran en la terminal para su análisis.

## 9. Consideraciones

Los datos de sensores son simulados con fines académicos. Los resultados del procesamiento por lotes deben describirse de acuerdo con el archivo de datos realmente utilizado.

Este repositorio documenta el desarrollo práctico de la Tarea 3 del curso Big Data de la UNAD.

