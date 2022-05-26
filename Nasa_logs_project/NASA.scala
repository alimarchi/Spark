// Databricks notebook source
import org.apache.spark.sql.SparkSession

val spark = SparkSession
    .builder
    .appName("NASA")
    .getOrCreate()
    
val file = ("/FileStore/tables/NASA_access_log_Jul95.gz")

val nasa_df = spark.read.text(file)

// COMMAND ----------

nasa_df.printSchema()

// COMMAND ----------

nasa_df.show(10, truncate = false)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC * Host: 199.72.81.55
// MAGIC * User-identifier: "-", significa que faltan esos datos, por lo que obviaremos este campo.
// MAGIC * Userid:  "-", significa que faltan esos datos, por lo que obviaremos este campo.
// MAGIC * Date: [01/Jul/1995:00:00:01 -0400] dd/MMM/yyyy:HH:mm:ss y el campo final “-0400” sería el timezone que en este caso omitiremos, además haremos una transformación de los meses a forma numérica.
// MAGIC * Request Method: GET
// MAGIC * Resource: /history/apollo/ HTTP/1.0, sería el recurso al que se accede en esta petición.
// MAGIC * Protocol: HTTP/1.0, y por ultimo en esta parte entre comillas tendríamos el protocolo utilizado al ser logs de 1995, seguramente sea el único protocolo utilizado.
// MAGIC * HTTP status code: 200, existen distintos códigos de estado de HTTP.
// MAGIC * Size: 6245, y como ultimo campo tendríamos el tamaño del objeto recibido por el cliente en bytes. En casos de error del cliente, este campo no se encuentra por lo que al igual que en los userid, será indicado con un “-“, tenerlo en cuenta.

// COMMAND ----------

nasa_df.count()

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Primero debemos cargar el archivo como un archivo de texto normal y realizar las transformaciones pertinentes, a la hora de limpiar y estructurar nuestro dataset utilizaremos expresiones regulares para recoger los campos que necesitamos. 

// COMMAND ----------

val host = "(\\S+)"
val date = "\\[(\\S+)\\s"
val request = """\"(\w+)\s"""
val resource = "\\s(\\/\\S+)\\s"
val protocol = """\s(\bHT\S+)\""""
val status = "\\s(\\d{3})\\s"
val content_size = "\\s(\\d+)\\Z"

// COMMAND ----------

import org.apache.spark.sql.functions.regexp_extract
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

val logs_df = nasa_df.select(regexp_extract(col("value"), host, 1).alias("host"),
                         regexp_extract(col("value"), date, 1).alias("date"),
                         regexp_extract(col("value"), request, 1).alias("request_method"),
                         regexp_extract(col("value"), resource, 1).alias("resource"),
                         regexp_extract(col("value"), protocol, 1).alias("protocol"),
                         regexp_extract(col("value"), status, 1).cast("integer").alias("status"),
                         regexp_extract(col("value"), content_size, 1).cast("integer").alias("content_size"))

// COMMAND ----------

logs_df.show(10, truncate = false)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Guardaremos nuestro nuevo DataFrame ya estructurado en formato parquet. Y de este leeremos para realizar nuestro análisis.

// COMMAND ----------

logs_df.write.parquet("/tmp/output/nasa.parquet")

// COMMAND ----------

val logs = spark.read.format("parquet")
 .option("header", "true")
 .option("inferSchema", "true")
 .load("/tmp/output/nasa.parquet")

// COMMAND ----------

logs.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ¿Cuáles son los distintos protocolos web utilizados? Agrúpalos

// COMMAND ----------

import org.apache.spark.sql.functions.col

logs.groupBy("protocol")
    .count()
    .distinct()
    .sort(col("count").desc)
    .show()

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ¿Cuáles son los códigos de estado más comunes en la web? Agrúpalos y ordénalos para ver cuál es el más común.

// COMMAND ----------

logs.groupBy("status")
    .count()
    .distinct()
    .sort(col("count").desc)
    .show()

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ¿Y los métodos de petición (verbos) más utilizados?

// COMMAND ----------

logs.groupBy("request_method")
    .count()
    .distinct()
    .sort(col("count").desc)
    .show()

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Date: 01/Aug/1995:00:00:23 -0400, como podemos ver está en formato dd/MMM/yyyy:HH:mm:ss y el campo final “-0400” sería el timezone que en este 
// MAGIC caso omitiremos, además haremos una transformación de los meses a forma numérica.

// COMMAND ----------

import org.apache.spark.sql.functions._

val logs_time = logs.withColumn("date_timestamp",to_timestamp(col("date"), "dd/MMM/yyyy:HH:mm:ss"))


// COMMAND ----------

logs_time.printSchema()

// COMMAND ----------

logs_time.show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ¿Qué recurso tuvo la mayor transferencia de bytes de la página web?

// COMMAND ----------

logs.select("resource", "content_size")
    .distinct()
    .sort(col("content_size").desc)
    .show(10, false)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC Además, queremos saber que recurso de nuestra web es el que más tráfico recibe. Es decir, el recurso con más registros en nuestro log.

// COMMAND ----------

logs.groupBy("resource")
    .count()
    .sort(col("count").desc)
    .show(10, false)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ¿Qué días la web recibió más tráfico?

// COMMAND ----------

logs_time.select(date_format(col("date_timestamp"), "yyyy-MM-dd").alias("day"))
         .groupBy("day")
         .count()
         .sort(col("count").desc)
         .show(10, false)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ¿Cuáles son los hosts son los más frecuentes?

// COMMAND ----------

logs.groupBy("host")
    .count()
    .sort(col("count").desc)
    .show(10, false)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ¿A qué horas se produce el mayor número de tráfico en la web?

// COMMAND ----------

logs_time.select(date_format(col("date_timestamp"), "HH:mm:ss").alias("time"))
         .groupBy("time")
         .count()
         .sort(col("count").desc)
         .show(10, false)

// COMMAND ----------

logs_time.select(date_format(col("date_timestamp"), "HH").alias("hour"))
         .groupBy("hour")
         .count().sort(col("count").desc)
         .show(10, false)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ¿Cuál es el número de errores 404 que ha habido cada día?

// COMMAND ----------

logs_time.filter(col("status") === 404).show(10)

// COMMAND ----------

logs_time.filter(col("status") === 404)
         .select(date_format(col("date_timestamp"), "yyyy-MM-dd").alias("day"))
         .groupBy("day")
         .count()
         .sort(col("count").desc)
         .show(10)  

// COMMAND ----------

logs_time.filter(col("content_size").isNull).count()

// COMMAND ----------

val new_logs = logs_time.na.fill(0)

// COMMAND ----------

new_logs.filter(col("content_size").isNull).count()
