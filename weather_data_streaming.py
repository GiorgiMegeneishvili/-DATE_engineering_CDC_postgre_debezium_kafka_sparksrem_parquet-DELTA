"""
PostgreSQL
    |
Debezium
    |
Kafka
    |
Spark Structured Streaming
    |
Delta Lake Bronze
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType, LongType
)
from pyspark.sql.functions import (
    col, from_json, from_unixtime, to_timestamp, current_timestamp,
    row_number, desc
)
from pyspark.sql.window import Window
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable


# =====================================================
# CONFIG
# =====================================================
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "postgres.public.weather_data"
DELTA_PATH = "/home/gm/Desktop/bronze_layer/weather_data"
CHECKPOINT_PATH = "/home/gm/Desktop/bronze_layer/_checkpoints/weather_data"


# =====================================================
# LOGGING
# =====================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("weather_cdc")


# =====================================================
# SPARK SESSION
# =====================================================
def create_spark():
    builder = SparkSession.builder \
        .appName("Weather_CDC_Bronze") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.shuffle.partitions", "8")

    return configure_spark_with_delta_pip(
        builder,
        extra_packages=["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8"]
    ).getOrCreate()


# =====================================================
# SCHEMAS
# =====================================================
weather_schema = StructType([
    StructField("id", IntegerType()),
    StructField("city", StringType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", IntegerType()),
    StructField("weather", StringType()),
    StructField("event_time", LongType()),
    StructField("created_at", LongType())
])

debezium_schema = StructType([
    StructField("payload", StructType([
        StructField("op", StringType()),
        StructField("before", weather_schema),
        StructField("after", weather_schema),
        StructField("ts_ms", LongType())
    ]))
])


# =====================================================
# HELPERS
# =====================================================
def convert_dates(df):
    return df \
        .withColumn("event_time", to_timestamp(from_unixtime(col("event_time") / 1000000))) \
        .withColumn("created_at", to_timestamp(from_unixtime(col("created_at") / 1000000))) \
        .withColumn("temperature", col("temperature").cast("decimal(5,2)"))


# =====================================================
# MICRO BATCH PROCESSING
# =====================================================
def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    logger.info(f"PROCESSING BATCH {batch_id}")
    spark = batch_df.sparkSession
    delta_table = DeltaTable.forPath(spark, DELTA_PATH)

    # ==================== UPSERT ====================
    upsert_df = batch_df \
        .filter(col("op").isin("c", "r", "u")) \
        .filter(col("after").isNotNull()) \
        .select("after.*", "ts_ms")

    if not upsert_df.isEmpty():
        window = Window.partitionBy("id").orderBy(desc("ts_ms"))

        upsert_df = upsert_df \
            .withColumn("rn", row_number().over(window)) \
            .filter(col("rn") == 1) \
            .drop("rn")

        upsert_df = convert_dates(upsert_df)

        logger.info("UPSERT rows:")
        upsert_df.show(truncate=False)

        delta_table.alias("target") \
            .merge(upsert_df.alias("source"), "target.id = source.id") \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()

        logger.info("UPSERT DONE")

    # ==================== DELETE ====================
    delete_df = batch_df \
        .filter(col("op") == "d") \
        .filter(col("before").isNotNull()) \
        .select(col("before.id").alias("id"), col("ts_ms"))

    if not delete_df.isEmpty():
        logger.info("DELETE EVENTS:")
        delete_df.show()

        delta_table.alias("target") \
            .merge(delete_df.alias("source"), "target.id = source.id") \
            .whenMatchedDelete() \
            .execute()

        logger.info("DELETE DONE")


# =====================================================
# MAIN
# =====================================================
def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    logger.info("WEATHER CDC STREAM STARTED")

    # Read from Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    # Parse Debezium JSON
    parsed_df = kafka_df \
        .filter(col("value").isNotNull()) \
        .selectExpr("CAST(value AS STRING) json") \
        .select(from_json(col("json"), debezium_schema).alias("data")) \
        .select("data.payload.*") \
        .withColumn("_processed_at", current_timestamp())

    # Write Stream
    query = parsed_df \
        .writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .trigger(processingTime="10 seconds") \
        .start()

    query.awaitTermination()


#if __name__ == "__main__":
 #   main()
