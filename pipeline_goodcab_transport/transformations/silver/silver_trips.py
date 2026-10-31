
from pyspark import pipelines as dp
from pyspark.sql.functions import *

catalog=spark.conf.get("catalog")

dq_rules = {
    "valid_trip_id": "trip_id IS NOT NULL",
    "valid_city_id": "city_id IS NOT NULL",
    "valid_passenger_rating": "passenger_rating BETWEEN 1 AND 10",
    "valid_driver_rating": "driver_rating BETWEEN 1 AND 10"
}

@dp.temporary_view(name="tempview_silver_trips")
@dp.expect_all(dq_rules)
def silver_trips():

    df = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{catalog}.bronze.bronze_trips")
        .select(
            col("trip_id").cast("string"),
            col("date").cast("date"),
            col("city_id").cast("string"),
            col("passenger_type").cast("string"),
            col("distance_travelled_km").cast("double"),
            col("fare_amount").cast("double"),
            col("passenger_rating").cast("int"),
            col("driver_rating").cast("int"),
            col("start_time").cast("timestamp"),
            col("end_time").cast("timestamp"),
            col("bike_id").cast("int"),
            col("duration").cast("int"),
            col("bronze_ingest_datetime").cast("timestamp"),

            # CDF metadata
            col("_change_type"),
            col("_commit_version")
        )
        # Keep one event per key per commit version: drop update_preimage rows
        # (update_postimage carries the new values) so Auto CDC sequencing stays valid.
        .filter(col("_change_type").isin("insert", "update_postimage", "delete"))
        .withColumn("silver_ingest_datetime", current_timestamp())
    )

    return df


dp.create_streaming_table(
    name=f"{catalog}.silver.silver_trips",
    comment="Streaming table transportation.silver.silver_trips",
    table_properties={
        "layer": "silver",
        "quality": "clean",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)


dp.create_auto_cdc_flow(
    source="tempview_silver_trips",
    target=f"{catalog}.silver.silver_trips",
    keys=["trip_id"],
    sequence_by="_commit_version",
    apply_as_deletes=expr("_change_type = 'delete'"),
    apply_as_truncates=expr("false"),
    stored_as_scd_type=1,
    except_column_list=[
        "_change_type",
        "_commit_version"
    ]
)
