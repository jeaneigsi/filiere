from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, from_json, col, avg, window
from pyspark.sql.types import StructType, StringType, FloatType

# 1. Définir le schéma du message JSON
schema = StructType() \
    .add("machine_id", StringType()) \
    .add("valeur", FloatType()) \
    .add("timestamp", StringType()) \
    .add("type_capteur", StringType())

# 2. Initialiser la session Spark avec accès à MinIO
spark = SparkSession.builder \
    .appName("raffinerie-iot") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# 3. Lire les données depuis Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "sensor-data") \
    .load()

# 4. Convertir les messages JSON
json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# 5. Sauvegarde brute dans MinIO (au format JSON compressé)
json_df.writeStream \
    .format("json") \
    .option("path", "s3a://raffinerie-raw/raw") \
    .option("checkpointLocation", "s3a://raffinerie-raw/checkpoint_raw") \
    .outputMode("append") \
    .start()

# 6. Filtrer les données valides
filtrees = json_df.filter(
    ((col("type_capteur") == "temperature") & (col("valeur").between(30, 150))) |
    ((col("type_capteur") == "vibration") & (col("valeur").between(0, 5)))
)

# 7. Fonction batch pour enregistrer les mesures filtrées dans TimescaleDB
def save_filtrees_to_pg(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://timescaledb:5432/iotdb") \
        .option("dbtable", "mesures_filtrees") \
        .option("user", "admin") \
        .option("password", "admin") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

# 8. Écriture batch des données filtrées
filtrees.writeStream \
    .foreachBatch(save_filtrees_to_pg) \
    .outputMode("append") \
    .option("checkpointLocation", "/app/data/checkpoint_filtrees") \
    .start()

# 9. Calcul KPI : moyenne glissante sur 1 minute
kpi = filtrees.withColumn("ts", expr("to_timestamp(timestamp)")) \
    .withWatermark("ts", "30 seconds") \
    .groupBy(window("ts", "1 minute"), "type_capteur") \
    .agg(avg("valeur").alias("valeur")) \
    .withColumn("type_kpi", col("type_capteur")) \
    .withColumn("unite", expr("CASE WHEN type_capteur = 'temperature' THEN '°C' ELSE 'mm/s' END")) \
    .selectExpr("window.start as timestamp", "type_kpi", "valeur", "unite")

# 10. Fonction batch pour enregistrer les KPI dans TimescaleDB
def save_kpi_to_pg(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://timescaledb:5432/iotdb") \
        .option("dbtable", "kpi_indicateurs") \
        .option("user", "admin") \
        .option("password", "admin") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

# 11. Écriture batch des KPI
kpi.writeStream \
    .foreachBatch(save_kpi_to_pg) \
    .outputMode("append") \
    .option("checkpointLocation", "/app/data/checkpoint_kpi") \
    .start() \
    .awaitTermination()