from pyspark import pipelines as dp
from pyspark.sql import *
from pyspark.sql.functions import *


dq_rules={'valid_trip_id':'trip_id IS NOT NULL',
'valid_city_id': 'city_id IS NOT NULL',
'valid_passenger_rating': 'passenger_rating BETWEEN 1 AND 10',
'valid_driver_rating': 'driver_rating BETWEEN 1 AND 10'}


@dp.temporary_view(
    name='tempview_silver_trips',
    comment='temperory view transportation.silver.silver_trips'
)
@dp.expect_all(dq_rules)

def silver_trips():
    df=(
        spark.readStream.table('transportation.bronze.bronze_trips')
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
            col("bronze_ingest_datetime").cast("timestamp")
        )
        .withColumn('silver_ingest_datetime', current_timestamp())
    )
    return df


dp.create_streaming_table(
    name='transportation.silver.silver_trips',
    comment='Streaming table transportation.silver.silver_trips',
    table_properties={
        'layer': 'bronze',
        'quality': 'raw',
        'delta.enableChangeDataFeed': 'true',
        'delta.autoOptimize.optimizeWrite': 'true',
        'delta.autoOptimize.autoCompact': 'true'
    }
)


dp.create_auto_cdc_flow(
    source='tempview_silver_trips',
    target='transportation.silver.silver_trips',
    keys=['trip_id'],
    sequence_by='bronze_ingest_datetime',
    stored_as_scd_type=1,
    except_column_list=[]
)
