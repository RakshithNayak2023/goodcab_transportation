from pyspark import pipelines as dp
from pyspark.sql import *
from pyspark.sql.functions import *

catalog=spark.conf.get("catalog")

@dp.materialized_view(
    name=f"{catalog}.silver.silver_city",
    comment='This is a silver table',
    table_properties={
        'quality':'stage',
        'layer':'silver',
        'delta.autoOptimize.optimizeWrite':'true',
        'delta.autoOptimize.autoCompact':'true',
    }
)

def silver_city():
    df=(
        spark.read.table(f"{catalog}.bronze.bronze_city").
        select('city_id','city_name','bronze_ingest_datetime').
        withColumn('silver_ingest_timestamp',current_timestamp())
    )
    return df
