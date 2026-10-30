from pyspark import pipelines as dp
from pyspark.sql import *
from pyspark.sql.functions import *

PATH=spark.conf.get("transportation.bronze.trips")
SCHEMA_PATH = spark.conf.get("transportation.bronze.schema.trips")


from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.table(
    name="transportation.bronze.bronze_trips",
    comment="Bronze streaming table for trips data",
    table_properties={
        'layer': 'bronze',
        'quality': 'raw',
        'delta.enableChangeDataFeed': 'true',
        'delta.autoOptimize.optimizeWrite': 'true',
        'delta.autoOptimize.autoCompact': 'true'
    }
)

def bronze_trips():
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", SCHEMA_PATH)
        .option("cloudFiles.schemaEvolutionMode", "failOnNewColumns")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("rescuedDataColumn", "rescuedData")
        .option("cloudFiles.allowOverwrites", "true")
        .option("cloudFiles.maxFilesPerTrigger", "10")
        .option("columnNameOfCorruptRecord", "corrupt_record")
        .option("mode", "PERMISSIVE")
        .option(
            "cloudFiles.schemaHints",
            "trip_id STRING, start_time TIMESTAMP, "
            "end_time TIMESTAMP, bike_id STRING, duration STRING ,corrupt_record STRING "
        )
        .load(PATH)
        .withColumn("bronze_ingest_datetime", current_timestamp())
        .withColumn("file_path", col("_metadata.file_path"))
        .withColumnRenamed(
            "distance_travelled(km)",
            "distance_travelled_km"
        )
    )

    return df