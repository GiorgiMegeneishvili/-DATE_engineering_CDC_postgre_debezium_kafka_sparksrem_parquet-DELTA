from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


# =========================
# SPARK SESSION WITH DELTA
# =========================

builder = SparkSession.builder \
    .appName("Read_Delta_Table") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()


# =========================
# READ DELTA TABLE
# =========================

person = spark.read.format("delta").load("/home/gm/Desktop/bronze_layer/person")
weather_data = spark.read.format("delta").load("/home/gm/Desktop/bronze_layer/weather_data")


person.where('business_entity_id in (10000579,10000565)').show()
weather_data.where(weather_data.id.isin([13715,743,744, 745,746])).show()

##weather_data.printSchema()
##weather_data.where("id in (744, 745)").show()
##person.printSchema()

#spark.read.format("delta").load("/home/gm/Desktop/bronze_layer/person").printSchema()
spark.stop()
