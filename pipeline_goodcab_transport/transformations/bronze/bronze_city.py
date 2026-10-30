from pyspark import pipelines as dp
from pyspark.sql import *
from pyspark.sql.functions import *

PATH=spark.conf.get("transportation.bronze.city")

@dp.materialized_view(
    name='transportation.bronze.bronze_city',
    comment='Bronze table for city data',
    table_properties={
        'layer': 'bronze',
        'quality': 'raw',
        'delta.enableChangeDataFeed': 'true',
        'delta.autoOptimize.optimizeWrite': 'true',
        'delta.autoOptimize.autoCompact': 'true',
    }
)
def bronze_city():
    df = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load(PATH)
    df=df.withColumn('file_path', col('_metadata.file_path')).withColumn('bronze_ingest_datetime', current_timestamp())
    return df