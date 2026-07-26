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
    StructType, StructField, IntegerType, StringType, LongType, BooleanType
)
from pyspark.sql.functions import (
    col, from_json, from_unixtime, to_timestamp, date_add,
    lit, current_timestamp, row_number, desc
)
from pyspark.sql.window import Window
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable


# =====================================================
# CONFIGURATION
# =====================================================
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "postgres.public.person"

DELTA_PATH = "/home/gm/Desktop/bronze_layer/person"
CHECKPOINT_PATH = "/home/gm/Desktop/bronze_layer/_checkpoints/person"


# =====================================================
# LOGGING
# =====================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("person_cdc")


# =====================================================
# SPARK SESSION
# =====================================================
def create_spark():
    builder = SparkSession.builder \
        .appName("Person_CDC_Bronze") \
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
person_schema = StructType([
    StructField("business_entity_id", IntegerType()),
    StructField("person_type", StringType()),
    StructField("name_style", BooleanType()),
    StructField("title", StringType()),
    StructField("first_name", StringType()),
    StructField("middle_name", StringType()),
    StructField("last_name", StringType()),
    StructField("suffix", StringType()),
    StructField("email_promotion", IntegerType()),
    StructField("modified_date", LongType()),
    StructField("inserted_date", LongType())
])

debezium_schema = StructType([
    StructField("payload", StructType([
        StructField("op", StringType()),
        StructField("before", person_schema),
        StructField("after", person_schema),
        StructField("ts_ms", LongType())
    ]))
])


# =====================================================
# DATE CONVERSION
# =====================================================
def convert_dates(df):
    return df \
        .withColumn(
            "modified_date",
            date_add(lit("1970-01-01"), col("modified_date").cast("int"))
        ) \
        .withColumn(
            "inserted_date",
            to_timestamp(from_unixtime(col("inserted_date") / 1000000))
        )


# =====================================================
# BATCH PROCESSING
# =====================================================
def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    logger.info(f"PROCESSING BATCH {batch_id}")
    spark = batch_df.sparkSession
    delta_table = DeltaTable.forPath(spark, DELTA_PATH)

    # --------------------- UPSERT ---------------------
    upsert_df = batch_df \
        .filter(col("op").isin("c", "r", "u")) \
        .filter(col("after").isNotNull()) \
        .select("after.*", "ts_ms")

    if not upsert_df.isEmpty():
        window = Window.partitionBy("business_entity_id").orderBy(desc("ts_ms"))

        upsert_df = upsert_df \
            .withColumn("rn", row_number().over(window)) \
            .filter(col("rn") == 1) \
            .drop("rn")

        upsert_df = convert_dates(upsert_df)

        logger.info("UPSERT DATA")
        upsert_df.show(truncate=False)

        delta_table.alias("target") \
            .merge(upsert_df.alias("source"), "target.business_entity_id = source.business_entity_id") \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()

        logger.info("UPSERT DONE")

    # --------------------- DELETE ---------------------
    delete_df = batch_df \
        .filter(col("op") == "d") \
        .filter(col("before").isNotNull()) \
        .select(col("before.business_entity_id").alias("business_entity_id"))

    if not delete_df.isEmpty():
        logger.info("DELETE EVENTS")
        delete_df.show()

        delta_table.alias("target") \
            .merge(delete_df.alias("source"), "target.business_entity_id = source.business_entity_id") \
            .whenMatchedDelete() \
            .execute()

        logger.info("DELETE DONE")


# =====================================================
# MAIN
# =====================================================
def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    logger.info("PERSON CDC STREAM STARTED")

    # Read from Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    # Parse Debezium payload
    parsed_df = kafka_df \
        .filter(col("value").isNotNull()) \
        .selectExpr("CAST(value AS STRING) json") \
        .select(from_json(col("json"), debezium_schema).alias("data")) \
        .select("data.payload.*") \
        .withColumn("_processed_at", current_timestamp())

    # Start Streaming Query
    query = parsed_df \
        .writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .trigger(processingTime="10 seconds") \
        .start()

    query.awaitTermination()


#if __name__ == "__main__":
    #main()
